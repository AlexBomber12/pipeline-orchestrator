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

The opted-in diagnostics service also exposes `get_latest_cli_log`. It accepts
one configured `owner__repo` slug and returns a sanitized tail of the fixed
latest-log record written by completed coder CLI invocations. The default tail
is 8 KiB and callers may request at most 32 KiB. The service accepts the
producer 64 KiB byte budget plus its bounded six-byte UTF-8 replacement
expansion, redacts the complete bounded source before selecting the tail, and
reports source/output sizes, truncation, observation time, and remaining Redis
TTL. A producer `[truncated]` marker means the stored tail may begin inside a
credential value, so the service fails closed with
`cli_log_producer_truncated` instead of exporting that record. Authorization
headers, token, password, and passphrase assignments or long-option arguments
and their multiline shell groups, cookies, credential-bearing absolute or
scheme-relative URL userinfo/query parameters (including percent-encoded
parameter names, Azure SAS signatures, and AWS/Google presigned signatures),
recognizable token shapes including JWTs and Slack webhooks, private-key blocks,
PuTTY private-key documents, standalone AWS access-key identifiers and AWS
credential CSVs with quoted or unquoted fields and an optional leading UTF-8
BOM, curl user/proxy-user
credentials, netrc passwords delimited by horizontal whitespace or a newline,
and recognizable JSON
credential documents are not exported, including nested documents and documents
serialized inside log strings and structured header name/value pairs in JSON-list or
Python-tuple form. Recognizable Kubernetes Secret documents are omitted as a
whole in YAML or JSON form, including arbitrary keys under `data` or
`stringData`, decorated, explicit, quoted, or escaped `kind` keys, and `kind`
values that are anchored, aliased, tagged using non-specific, shorthand, or
verbatim forms, escaped double-quoted, node-property-decorated multiline, or
block-scalar
(including indentation indicators), multiline plain-scalar, block-sequence, or
flow-mapping `kind` values. Complete and interrupted private-key blocks are both
omitted using bounded boundary scans.
Standard encoded `auth` fields used by registry and package-manager credential
documents are treated as credential context rather than exported as base64 text.
YAML documents with explicit credential keys, alias mapping keys, or mapping
keys that use recognized YAML-only escape forms are omitted conservatively
rather than partially decoded. Node-property-only credential values retain their
context across blank and comment lines. Documents containing multiline explicit
single- or double-quoted mapping keys are omitted because folded keys cannot be
classified safely without interpreting YAML. Explicit block-scalar mapping keys
are omitted for the same reason.
Recognizable XML credential elements, credential attributes, and `key`/`name`
plus `value` configuration tags are omitted. XML character references in
credential selectors are decoded before classification. Plist-style sensitive
`key`/`name` element text is normalized across bounded attributed start tags,
comments, and CDATA before its following scalar value is omitted. Other or
incomplete selector markup fails
closed through the bounded source end. Multiline or incomplete XML credential
contexts fail closed through the bounded source end, and
unresolved named entities in credential selectors are treated as ambiguous
credential context. DTD-bearing XML fails closed from the declaration boundary
without parsing or expanding internal or external entities.
Standalone single-token authorization-scheme values such as `Bearer` and
`Basic` credentials are redacted even when the header name is absent; a
multi-parameter `Digest` value causes conservative line omission.
JSON inspection preserves duplicate object members so a later empty value cannot
hide an earlier credential value. Recognizable RSA, EC, OKP, and symmetric
private JWK objects are omitted while public JWKs remain exportable. PEM, SSH2,
and PuTTY private-key documents are omitted through their recognized boundaries;
incomplete blocks fail closed.
Lines with recognizable credential keys are omitted conservatively when they
use assignment, structured-field, header, or long-option syntax. Balanced
quotes around recognized multiword credential labels are supported, so
malformed or interrupted quoting cannot expose a value suffix. Indented YAML/header
continuations, backslash-continued shell values, and quoted values spanning
physical lines are omitted with their key line through the close or source end.
Command, parameter, and backtick substitutions remain tracked inside
double-quoted credential values.
This includes leading blank lines and legal indentationless YAML sequence values
under a credential key, plus shell heredoc bodies through their delimiter or
source end. Literal heredoc delimiters may start with digits; unsupported
delimiter words fail closed by omitting the remainder of the bounded source.
YAML documents containing a sensitive field with a comment-only value, alias,
or flow-style collection are omitted as a whole because line-level redaction
cannot safely retain the referenced or indentationless value. Explicit document
boundaries preserve neighboring documents; without one, the bounded source
segment fails closed.
Terminal escape and control sequences, including C1 control strings, are
normalized before credential inspection. The source suffix beginning with the
first bare carriage return or stateful cursor/editing control is omitted
conservatively, preventing terminal overwrite semantics or lost multiline
context from exposing a hidden credential. Credential-key inspection normalizes
separated, camel-case, single-case compound, and bracketed parameter names,
quoted mapping subscripts, and common multiword labels such as `API key`; it uses
a bounded-source, single-pass line scanner so long non-credential lines do not
cause regex backtracking stalls.
Malformed or incomplete JSON containers with credential contexts are omitted
through their closing boundary or, when unterminated, through the source end;
legal whitespace may separate a credential key, delimiter, and value.
All legal JSON string escapes in credential keys are decoded during fallback
inspection.
Malformed JSON probing has a fixed failure budget; if that budget is
exhausted, export fails closed to a credential-document omission marker instead
of blocking the MCP event loop or returning text that could not be inspected
safely.

This legacy latest-only record has no trustworthy task, invocation, commit SHA,
or producer timestamp. Those associations are returned as unavailable and are
never inferred from the current pipeline snapshot or TTL. `observed_at` is only
the retrieval time. A missing Redis value can mean either never written or
expired; the response reports that ambiguity. An existing zero-length value is
reported as available with empty text. Reads use `STRLEN`, bounded `GETRANGE`,
`EXISTS`, and `TTL`; they do not refresh expiry, mutate state, access credential
files or caller-selected Redis keys, trigger Retry, or launch a process.

Historical CLI-log discovery, CI/event/artifact logs, process observations,
daemon stdout, and live output remain unavailable and require separate reviewed
contracts.

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
  the allowlisted diagnostics tools, and call `get_orchestrator_status` and
  `get_latest_cli_log`. Confirm the latter returns only its bounded, sanitized
  latest-log tail and that no historical-log, artifact, event, credential-file,
  arbitrary-file, arbitrary-key, or mutation tool appears.
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
