# Claude Code browser-login transport investigation

Investigation date: 2026-10-10. Repository base:
`59e3a814ec03fac5ea22057b960cffd45eb85604` (merged PR #587).

This document records evidence for a future Settings login flow. It does not
enable login. Claude's flow is **browser OAuth with an authorization code pasted
back into the CLI**; it is not device-code authentication.

## Versions and evidence

| Context | Version | Provenance and conclusion |
| --- | --- | --- |
| Editor host | 2.1.47 | `claude --version`; npm package `@anthropic-ai/claude-code@2.1.47` at `/home/alexey/.nvm/versions/node/v24.11.1/…/cli.js` (SHA-256 `2f9b383151697cb6a4020d34ef18e114697fa03f34153f2a93369271cc116a32`). This is not the daemon deployment. |
| Repository pin | 2.1.119 | `Dockerfile` line 29. This is the version a default build requests, not proof of a deployed image. |
| Disposable current image | 2.1.119 | Isolated `pipeline-orchestrator:codex-0.160.0-default` image `sha256:e284ba…`; `/usr/bin/claude` resolves to the packaged native binary. No auth mounts were attached. |
| Cached legacy images | 2.1.112 | Isolated version checks of locally cached `pipeline-orchestrator-{web,daemon,ci}:latest`. None was running, so these are not deployment evidence. |
| Running deployment | unknown | Only the Redis container was running on the investigation host. No running web or daemon binary could be checked. Compose builds locally and carries no immutable application-image/version reference. |
| Reference package | 2.1.126 | Disposable npm execution of `@anthropic-ai/claude-code@2.1.126`, registry integrity `sha512-eLuqO0iiXjQUipXQQEHBoCXG1CdxG+VBazV5sc8eA6HeRU18ur1UoL6xDrS1GA5A3IgIkgIFa9OMrJSVosdi6w==`. It was not installed into the host, repository, or deployment. |

The official [authentication guide](https://code.claude.com/docs/en/authentication)
describes copying the browser URL and pasting a code when localhost callbacks
fail in WSL2, SSH, or containers. The [week 18 release note](https://code.claude.com/docs/en/whats-new/2026-w18)
and [2.1.126 changelog](https://code.claude.com/docs/en/changelog#2-1-126)
attribute terminal code entry to 2.1.126. The [CLI reference](https://code.claude.com/docs/en/cli-reference#cli-commands)
documents `claude auth login` but no noninteractive-code flag.

Therefore neither available installed version (2.1.47 nor 2.1.119) can be
treated as supporting the documented fallback. Newer documentation is not
evidence for those binaries. A future implementation must require Claude Code
2.1.126 or later and fail closed for an older, missing, or unparseable version.

## Sanitized runtime observations

Each probe ran in a disposable container with a fresh `HOME` and
`CLAUDE_CONFIG_DIR`, no host mounts, no inherited provider credentials, and
`BROWSER=/bin/false`. No browser was opened, no real code was entered, and no
token exchange or inference was completed. Every run had an external deadline.
Raw output and full URL query values were captured only in temporary files,
reduced to the facts below, and shredded. Owned containers were then confirmed
absent; one PTY run required an explicit stop/removal after its deadline.

| Binary and transport | Direct observation |
| --- | --- |
| 2.1.119, ordinary pipes | Printed `Opening browser to sign in…` and an HTTPS authorization URL, then remained alive until the 12-second deadline. It did not print a manual-code prompt. |
| 2.1.126, ordinary pipes | Printed the browser URL and `Paste code here if prompted >`; no TTY error occurred. A synthetic, delayed newline-framed input was not consumed and the process remained alive until the deadline. |
| 2.1.126, PTY | Printed the same phases. The same synthetic line was consumed and produced `Invalid code. Please make sure the full code was copied.` The process then waited for more input until the deadline. |

The observed authorization URL origin/path was
`https://claude.com/cai/oauth/authorize`. Only query-key names were retained:
`client_id`, `code`, `code_challenge`, `code_challenge_method`, `redirect_uri`,
`response_type`, `scope`, and `state`. Values are intentionally omitted.

These observations establish:

- Browser URL output works in a remote/container process without a browser.
- Manual code entry exists in 2.1.126 and covers an unreachable localhost
  callback, matching the official release note.
- Output can be captured through pipes, but the required input path was not
  usable through ordinary pipes in the bounded probe. A PTY consumed the same
  input and is therefore required for this integration.
- Input framing is one opaque code followed by a terminal newline. The daemon
  must never interpret it as a device code or OAuth token.
- ANSI sequences, carriage-return redraws, and spinner frames occur on the PTY.
  The adapter must normalize those before matching allowlisted phases.
- External cancellation can be bounded and cleaned up, but graceful CLI exit,
  successful token exchange, provider expiry, and post-success prompts remain
  unverified because completing a real login was excluded.

## Expected command and phases

Run `claude auth login` (Claude subscription is its default account type) in a
daemon-owned PTY. Do not use `claude setup-token`, implement OAuth independently,
or expose a browser terminal.

The supported state sequence should be:

1. Start the CLI under the existing process-group supervisor and capture the PTY
   master. Apply an application deadline before returning control to Settings.
2. Normalize terminal output and allowlist the HTTPS origin, path, and query-key
   set before returning an authorization URL. Never log the raw URL.
3. Enter `waiting_for_code` only after observing the paste prompt. Settings opens
   the URL on the operator's workstation and submits one opaque code.
4. Write the bounded code plus `\n` once to the PTY master. Do not echo, retain,
   return, or include it in errors.
5. Let Claude Code perform the OAuth/PKCE exchange. On clean exit, run the
   existing read-only auth probe and require saved credentials to be present
   before reporting success.

The exact success text, whether a final Enter is required by the standalone
`auth login` command, valid-code rejection behavior, provider expiry, and the
effect of starting login over existing credentials are unverified. Fixture
parsers must not invent those facts; a later controlled-account smoke test is
needed before declaring the end-to-end path production-ready.

## Credential ownership and location

The CLI must own the OAuth exchange and credential persistence. Anthropic's
authentication guide states that Linux credentials are written to
`.credentials.json` beneath `CLAUDE_CONFIG_DIR` when that variable is set. The
application should neither receive tokens nor write credential files.

For this repository, normalize `auth.claude_config_dir` (currently
`/data/auth/claude`) as the credential-location identity and build one immutable
environment that preserves the daemon's inherited `HOME` while setting
`CLAUDE_CONFIG_DIR` to that exact location. Use it for login, pre/post auth
probes, model discovery, usage reads, and coder execution. Discovery already
sets this directory, but `ClaudePlugin.check_auth` and
`claude_cli.run_claude_async` must accept the reservation-bound environment
instead of reconstructing it after a reservation is taken.

Reuse `CoderCredentialReservations` unchanged in policy: login is exclusive;
auth, catalog, usage, and coder processes are readers; credential generation
advances only after confirmed cleanup. Reuse the current replacement-consent
gate. Until a real probe establishes Claude's overwrite behavior, its warning
must say that failed or cancelled replacement may leave the prior login
unavailable rather than promise preservation.

## Smallest implementation extension

Preserve every Codex device-login contract and endpoint. Add a distinct
`browser_code` optional capability and parallel browser-login payload; do not
rename or reinterpret `device_code`, `verification_url`, or `user_code`.

- **Plugin contract:** add a browser-code adapter with command, immutable
  environment/location, timeout, URL/prompt parser, PTY requirement, and
  sanitized failure classifier. Advertise `browser_code` only from a successful
  version probe at 2.1.126 or later; the factory repeats the version guard so a
  stale UI cannot start an unsupported binary. Keep the static fallback empty
  when version support is unknown.
- **Process and manager:** add a small PTY transport layered on
  `launch_process`/`SupervisedProcess`, not a new supervisor. Extend
  `CoderLoginSessionManager` with browser-code start, inspect, one-shot submit,
  cancel, expiry, and shutdown paths. Reuse session capacity, opaque IDs,
  reservations, replacement consent, output bounds, TERM/KILL cleanup, retained
  ownership on uncertain cleanup, post-success auth refresh, and terminal-state
  retention. The PTY master closes only after cleanup is confirmed.
- **Wire payload:** expose only plugin, session ID, state, sanitized detail,
  failure reason, validated authorization URL, expiry/application deadline,
  cleanup status, replacement fields, and post-success auth status. Never return
  submitted code, raw terminal output, PKCE/state values separately, or tokens.
- **Redis bridge:** add `browser_login_start`, `browser_login_inspect`,
  `browser_login_submit`, and `browser_login_cancel`. Put submitted code in a
  random, short-TTL one-time Redis key and queue only its opaque handle; daemon
  consumption uses `GETDEL`, and every error path deletes it. The code must not
  appear in queue JSON, response keys, logging, diagnostics, or exceptions.
- **Settings:** add separate browser-login routes and a component selected only
  by `browser_code`. Render the validated URL, then a masked, non-autocomplete
  code field after `waiting_for_code`. Submit once, clear the field immediately,
  poll only by session ID, and reuse the existing cancellation, unresolved
  cleanup, expiry, replacement-warning, and auth/catalog refresh behavior. Store
  neither URL nor code in browser storage or analytics.

Unsupported versions return a sanitized `unsupported_cli_version` result with
the observed version and minimum version, expose no start control, and never
fall back to device login. Codex continues to advertise and use its existing
`device_code` implementation through ordinary pipes.

## Subsequent implementation tasks

Each item is intended as a separate small PR; estimates are additions plus
deletions and should be refined against the canonical task schema.

1. **Pin the prerequisite CLI (depends on none).** Expected: `Dockerfile`,
   `tests/test_dockerfile_pins.py`, upgrade runbook only if its evidence format
   changes. Estimate: production/config 2 lines, tests 10, docs 10; total 22.
   Acceptance:
   an isolated built image reports exactly 2.1.126 (or a separately investigated
   later pin), help remains compatible, and no deployment is performed.
2. **Add supervised PTY transport (depends on 1).** Expected:
   `src/process_supervisor.py`, `tests/test_process_supervisor.py`. Estimate:
   production 130, tests 220; total 350. Acceptance: incremental read/write, newline input,
   output bounds, descriptor closure, timeout/cancellation, process-group cleanup,
   and retained ownership on uncertain cleanup are covered without changing pipe
   callers.
3. **Add the dormant Claude adapter and credential binding (depends on 1-2).**
   Expected: `src/coder_registry.py`, `src/coders/claude.py`,
   `src/claude_cli.py`, and their existing tests. Estimate: production 180,
   tests 320; total 500. Acceptance: exact version gate, URL/query allowlist, terminal
   normalization, `browser_code` metadata, identical credential location across
   login/readers/execution, and no activation for older/unknown versions.
4. **Extend the login manager (depends on 3).** Expected:
   `src/coder_login.py`, `tests/test_coder_login.py`. Estimate: production 160,
   tests 280; total 440. Acceptance: start/inspect/one-shot submit/cancel,
   replacement and reservation conflicts, deadlines, shutdown, cleanup failure,
   PTY descriptor cleanup, and Codex device-login regression coverage.
5. **Extend the Redis bridge (depends on 4).** Expected:
   `src/model_catalog_bridge.py`, `tests/test_model_catalog_bridge.py`. Estimate:
   production 100, tests 180; total 280. Acceptance: all four browser-login
   operations, short-TTL `GETDEL` input handling, no code in queue JSON,
   payloads, logs, or errors, and unchanged device-login operations.
6. **Add Settings API and capability gating (depends on 5).** Expected:
   `src/web/routes/settings.py`, `tests/test_settings.py`. Estimate: production
   100, tests 180; total 280. Acceptance: strict JSON/code bounds, honest status codes,
   unsupported-version behavior, sanitized responses, and unchanged device-login
   routes.
7. **Add the Settings browser-code component (depends on 6).** Expected:
   `src/web/templates/components/settings_coders.html`, a new browser-login
   component, `src/web/templates/settings.html`, and `tests/test_settings.py`.
   Estimate: production/templates 170, tests 220; total 390. Acceptance: explicit start and
   replacement consent, safe link/code form, one-shot submission, bounded
   polling/cancel, no browser persistence, auth/catalog refresh, and no Codex UI
   regression.

The final implementation still requires a controlled, non-production account
smoke test to verify valid-code completion, credential replacement semantics,
the success/exit phase, and actual provider expiry. Those facts were deliberately
not obtained in this investigation.
