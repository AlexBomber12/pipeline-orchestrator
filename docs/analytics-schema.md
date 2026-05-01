# Merged-PR outcome log schema

The pipeline orchestrator writes one structured JSON row per merged PR
to `/data/analytics/<year>-<month>.jsonl`. The log is append-only,
partitioned by month, and has no upload or analysis layer — operators
inspect it with `jq`.

The schema is the foundation for future analytics (selector training,
cost-per-merged-PR metrics, lessons-learned across past PRs, opt-in
telemetry). Designing it up front lets future capabilities ship without
backfilling history.

> **Architectural decision (2026-04-29).** Lessons learned from past
> PRs are valid only for the *same* `coder` × `coder_model_string` ×
> `coder_extension_version` combination. Cross-version aggregation is
> unsafe because each model version has a different error distribution.
> Future analytics queries MUST default-filter to the current version;
> mixed-version reads are explicit operator opt-in.

## File layout

```
/data/analytics/
  2026-04.jsonl
  2026-05.jsonl
```

- One JSON object per line; trailing `\n`.
- `sort_keys=True` so byte diffs between rows are stable.
- Writes are guarded by `fcntl.flock` (LOCK_EX) so concurrent appends
  from a multi-daemon deployment never interleave bytes within a line.
- Override the directory via `PO_ANALYTICS_DIR` (used by the test suite
  to redirect into `tmp_path`).

## Fields

All 21 fields are present on every row. Missing data is written as JSON
`null`, never omitted — schema migration tools rely on the full key set.

| field | type | source | stability |
| --- | --- | --- | --- |
| `pr_id` | str | `tasks/QUEUE.md` task header | stable |
| `task_id_hash` | str | `sha256("{pr_id}::{repo_slug}")` (hex) | stable |
| `repo_slug` | str | runner `repo_slug_from_url(url)` | stable |
| `merged_at` | str | `datetime.now(UTC)` at merge | stable |
| `coder` | str | `repo.coder` or `daemon.coder` | stable |
| `coder_model_string` | str | `daemon.claude_model` / `daemon.codex_model` | stable; **critical for safe aggregation** |
| `coder_extension_version` \| `null` | str | `npm list -g --json <package>` | stable; `null` when detection fails |
| `task_type` | str | task header `Type:` field | stable; values from `_TASK_TYPE_VALUES` |
| `task_complexity` | str | task header `Complexity:` field | stable; one of `low`/`medium`/`high` |
| `fix_iterations` | int | `RunRecord.fix_iterations` | stable |
| `ci_runs_total` \| `null` | int | (deferred — currently `null`) | reserved |
| `ci_runs_failed` \| `null` | int | (deferred — currently `null`) | reserved |
| `wall_clock_seconds` \| `null` | int | `RunRecord.duration_ms / 1000` (CODING start → merge) | stable |
| `files_changed` | int | `RunRecord.files_touched_count` | stable |
| `lines_added` | int | `RunRecord.diff_lines_added` | stable |
| `lines_removed` | int | `RunRecord.diff_lines_deleted` | stable |
| `review_blocker_count` \| `null` | int | (deferred — heuristic too noisy currently) | reserved |
| `review_nit_count` \| `null` | int | (deferred — heuristic too noisy currently) | reserved |
| `codex_review_iterations` \| `null` | int | `PRInfo.fix_iteration_count + 1` (initial review + N fix passes) | stable |
| `tokens_estimate` \| `null` | int | (deferred — usage→record wiring not built) | reserved |
| `outcome` | str | always `"merged"` for now; reserved future values: `"abandoned"`, `"escalated"` | stable |

`task_id_hash` is the anonymizable identity field. Future opt-in
telemetry will ship the hash instead of `pr_id` + `repo_slug` so
external aggregators receive no repository or task identifiers in the
clear.

### Why `null` for some fields

`null` means "the daemon does not yet capture this signal in a way that
is reliable enough to log." Each reserved field requires a follow-up PR:

- `ci_runs_total` / `ci_runs_failed` — needs GHA check-runs aggregation
  per observed head SHA.
- `review_blocker_count` / `review_nit_count` — needs a stable
  `[P1]`/`[P2]`/`[blocker]`/`[nit]` classification convention from
  Codex; today the markers are inconsistent.
- `tokens_estimate` — needs `/api/oauth/usage` deltas tracked across
  the PR lifecycle; currently `RunRecord.tokens_in/out` is initialized
  to 0 and never updated.

Writing `null` (instead of `0`) keeps these distinguishable from
"genuinely zero" once the signals come online.

## Operator queries

```bash
# All fix-iteration counts for one coder/model combination.
cat /data/analytics/*.jsonl \
  | jq 'select(.coder == "claude" and .coder_model_string == "claude-opus-4-7") | .fix_iterations'

# Median fix iterations per task_type for a single model version.
cat /data/analytics/*.jsonl \
  | jq -s 'map(select(.coder_model_string == "claude-opus-4-7"))
           | group_by(.task_type)
           | map({task_type: .[0].task_type, median_fix_iters: (sort_by(.fix_iterations)[length/2|floor].fix_iterations)})'

# Wall-clock distribution for last month's merges.
cat /data/analytics/2026-04.jsonl | jq '.wall_clock_seconds' | sort -n | uniq -c
```

> **Aggregation hazard.** Never aggregate across `coder_model_string`
> values without explicit operator awareness. The recorded model
> version is the partition key for any meaningful comparison; pooling
> rows from `claude-opus-4-7` and `claude-opus-4-6` (or any future
> model bump) blends populations with different error distributions.

## Schema versioning

`OUTCOME_SCHEMA_VERSION` lives in `src/analytics/schema.py`. Bumping
rules:

- **Add a new field** at the end of `OUTCOME_FIELDS` and bump the
  version. Existing rows still parse — the new key is `null`-equivalent
  by absence (readers must tolerate either presence or absence).
- **Rename a field** is forbidden by the schema contract. Rename means
  add the new field, dual-write for one release, then deprecate the
  old field with a migration script over `/data/analytics/*.jsonl`.
- **Remove a field** requires the same migration strategy — readers in
  the wild may already depend on the key.

## Migration to SQLite

JSONL was chosen over SQLite for low data volume (~250 PR/year × ~500
bytes ≈ 125 KB/year) and operator inspection ergonomics (`jq`). When
storage scales up — cross-month or cross-tenant queries become slow,
or indexed filters on millions of rows are needed — a follow-up PR can
batch-INSERT the JSONL into SQLite. Because the schema is documented
here in advance, the migration script is mechanical: read line, parse
JSON, INSERT.
