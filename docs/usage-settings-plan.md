# Usage indicators in settings

## Outcome

Show provider quota observations in the matching coder plugin's settings card and GitHub REST/GraphQL observations in GitHub settings. The dashboard shows actionable inhibitors rather than permanent quota counters.

## Dependencies and boundaries

- `src/usage.py` owns external response parsing and its existing shared cache/backoff. Codex primary and secondary windows can independently be absent, as in the upstream Codex backend client. Missing data must never become zero usage.
- `RateLimitMixin` and `PipelineRunner.publish_state` own authenticated reads. Reuse credential reservations and shared providers. Publish one account-level observation per registered plugin through Redis; web reads observations without importing configured plugin factories or making provider requests.
- `usage_gate`, spend ceilings, reactive limit detection, fallback selection and GitHub throttling remain daemon policy. Optional windows require guards before comparison and reset arithmetic; configured threshold values and pause decisions for complete snapshots remain unchanged.
- Existing per-repository usage fields remain derived compatibility fields because runtime signatures/API consumers still read them. Settings must not derive account freshness from repository `last_updated`.
- Settings initial render, HTMX coder refresh/save responses and periodic usage refresh must agree. Moving GitHub auth out of the coder panel must preserve its health status. Unsupported plugins have no quota controls; unavailable and stale observations are explicit.
- Remove dashboard quota rendering, its obsolete aggregation and GraphQL burn display. Keep actual rate-limit/GitHub inhibitor messages and links to settings.
- No deployment, production pause, credential changes, scheduler changes or task-file edits.

## Implementation sequence

1. Accept independently present provider windows and expose typed observation status/timestamps. Add partial-window gate regressions before UI work.
2. Publish generic plugin observations using the existing daemon refresh path and Redis key helpers. Refresh continues while an active repository is paused; provider caches/backoff prevent requests per browser render. No new polling service.
3. Render coder observations and GitHub observations in settings, with a read-only HTMX refresh. Retain shared policy thresholds as a shared section.
4. Delete permanent dashboard counters and repair dependent route/template tests. Validate a third plugin and a plugin without quota support.
5. Run provider/gate, runner publication and settings/dashboard regressions, Chromium checks for DOM/HTMX, then the required final gate and uncommitted review artifacts. Publish one PR for review; GitHub owns integration execution.

## Success criteria

- No permanent quota rows on dashboard or repository detail.
- Settings supports any registered coder ID, including a plugin returning no usage provider.
- Missing windows, provider failures, stale observations and Redis failures do not fabricate a healthy counter or stop unrelated execution.
- Current complete-window pause, resume, spend-warning and GitHub throttle behavior remains covered.
- No coverage exclusions, weakened gates, runtime configuration changes or committed generated artifacts.

Provider access against real accounts and production reaping remain operator release checks. Mocked transport and the integration coder shim do not prove real provider access.
