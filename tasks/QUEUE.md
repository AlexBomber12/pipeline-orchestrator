# Task Queue

## PR-001: Project bootstrap
- Status: DONE
- Tasks file: tasks/PR-001.md
- Branch: pr-001-bootstrap

## PR-002: Config loader and data models
- Status: DONE
- Tasks file: tasks/PR-002.md
- Branch: pr-002-models
- Depends on: PR-001

## PR-003: Queue parser
- Status: DONE
- Tasks file: tasks/PR-003.md
- Branch: pr-003-queue-parser
- Depends on: PR-002

## PR-004: GitHub client
- Status: DONE
- Tasks file: tasks/PR-004.md
- Branch: pr-004-github-client
- Depends on: PR-002

## PR-005: Dashboard base layout and repo list
- Status: DONE
- Tasks file: tasks/PR-005.md
- Branch: pr-005-dashboard-base
- Depends on: PR-003, PR-004

## PR-006: Dashboard repo detail and HTMX polling
- Status: DONE
- Tasks file: tasks/PR-006.md
- Branch: pr-006-dashboard-detail
- Depends on: PR-005

## PR-007: Claude CLI wrapper
- Status: DONE
- Tasks file: tasks/PR-007.md
- Branch: pr-007-claude-cli
- Depends on: PR-002

## PR-008: Pipeline runner state machine
- Status: DONE
- Tasks file: tasks/PR-008.md
- Branch: pr-008-runner
- Depends on: PR-007, PR-003, PR-004

## PR-009: Daemon main loop
- Status: DONE
- Tasks file: tasks/PR-009.md
- Branch: pr-009-daemon-main
- Depends on: PR-008

## PR-010: Daemon recovery and error handling
- Status: DONE
- Tasks file: tasks/PR-010.md
- Branch: pr-010-recovery
- Depends on: PR-009

## PR-011: Repo scaffolding on connect
- Status: DONE
- Tasks file: tasks/PR-011.md
- Branch: pr-011-scaffolding
- Depends on: PR-009

## PR-012: Fix sprint 1 issues
- Status: DONE
- Tasks file: tasks/PR-012.md
- Branch: pr-012-sprint1-fixes
- Depends on: PR-006

## PR-013: Extract shared utils
- Status: DONE
- Tasks file: tasks/PR-013.md
- Branch: pr-013-utils
- Depends on: PR-012

## PR-014: Config writer and hot reload
- Status: DONE
- Tasks file: tasks/PR-014.md
- Branch: pr-014-config-writer
- Depends on: PR-013

## PR-015: Settings page - repositories
- Status: DONE
- Tasks file: tasks/PR-015.md
- Branch: pr-015-settings-repos
- Depends on: PR-014

## PR-016: Settings page - daemon and auth status
- Status: DONE
- Tasks file: tasks/PR-016.md
- Branch: pr-016-settings-daemon
- Depends on: PR-015

## PR-017: Dashboard observability - event log and stats
- Status: DONE
- Tasks file: tasks/PR-017.md
- Branch: pr-017-observability
- Depends on: PR-013

## PR-018: Dashboard alerts panel
- Status: DONE
- Tasks file: tasks/PR-018.md
- Branch: pr-018-alerts
- Depends on: PR-017

## PR-019: AGENTS.md layering and explicit Codex review trigger
- Status: DONE
- Tasks file: tasks/PR-019.md
- Branch: pr-019-agents-codex
- Depends on: PR-013

## PR-020: Stability fixes and cleanup
- Status: DONE
- Tasks file: tasks/PR-020.md
- Branch: pr-020-stability
- Depends on: PR-019

## PR-021: Daemon startup hardening
- Status: DONE
- Tasks file: tasks/PR-021.md
- Branch: pr-021-daemon-hardening
- Depends on: PR-020

## PR-022: IDLE open PR visibility
- Status: DONE
- Tasks file: tasks/PR-022.md
- Branch: pr-022-idle-pr
- Depends on: PR-021

## PR-023: Index page redesign - zero scroll repo cards
- Status: DONE
- Tasks file: tasks/PR-023.md
- Branch: pr-023-index-redesign
- Depends on: PR-022

## PR-024: Upload tasks and git push from dashboard
- Status: DONE
- Tasks file: tasks/PR-024.md
- Branch: pr-024-upload-tasks
- Depends on: PR-023

## PR-025: Start stop controls per repo
- Status: DONE
- Tasks file: tasks/PR-025.md
- Branch: pr-025-start-stop
- Depends on: PR-024

## PR-026: Tab title blink on alert
- Status: DONE
- Tasks file: tasks/PR-026.md
- Branch: pr-026-tab-blink
- Depends on: PR-023

## PR-027: Revert branch pre-creation and retry PR detection
- Status: DONE
- Tasks file: tasks/PR-027.md
- Branch: pr-027-revert-and-retry
- Depends on: PR-026

## PR-028: Claude CLI stdout logging
- Status: DONE
- Tasks file: tasks/PR-028.md
- Branch: pr-028-stdout-logging
- Depends on: PR-027

## PR-029: Fix stale review detection
- Status: DONE
- Tasks file: tasks/PR-029.md
- Branch: pr-029-stale-review
- Depends on: PR-027

## PR-030: QUEUE.md auto-update and squash merge
- Status: DONE
- Tasks file: tasks/PR-030.md
- Branch: pr-030-queue-done-squash
- Depends on: PR-027

## PR-031: Pre-merge sync with main
- Status: DONE
- Tasks file: tasks/PR-031.md
- Branch: pr-031-pre-merge-sync
- Depends on: PR-030

## PR-032: Subprocess helper and error handling hardening
- Status: DONE
- Tasks file: tasks/PR-032.md
- Branch: pr-032-subprocess-helper
- Depends on: PR-027

## PR-033: Dirty tree recovery and dedup codex review
- Status: DONE
- Tasks file: tasks/PR-033.md
- Branch: pr-033-dirty-recovery
- Depends on: PR-032

## PR-034: Model config, pytest in container, auth check
- Status: DONE
- Tasks file: tasks/PR-034.md
- Branch: pr-034-config-hardening
- Depends on: PR-032

## PR-035: Event log UX, card click, timestamps
- Status: DONE
- Tasks file: tasks/PR-035.md
- Branch: pr-035-dashboard-ux
- Depends on: PR-028

## PR-036: Dead code cleanup and CLAUDE.md fix
- Status: DONE
- Tasks file: tasks/PR-036.md
- Branch: pr-036-dead-code-cleanup
- Depends on: PR-035

## PR-041: Smart error handling and stderr logging
- Status: DONE
- Tasks file: tasks/PR-041.md
- Branch: pr-041-smart-errors
- Depends on: PR-036

## PR-037: Simplify queue-done and fix tech debt
- Status: DONE
- Tasks file: tasks/PR-037.md
- Branch: pr-037-simplify-queue-done
- Depends on: PR-041

## PR-038: Canonical repo identity
- Status: DONE
- Tasks file: tasks/PR-038.md
- Branch: pr-038-repo-identity
- Depends on: PR-041

## PR-039: Honest dashboard states
- Status: DONE
- Tasks file: tasks/PR-039.md
- Branch: pr-039-honest-states
- Depends on: PR-038

## PR-040: Rate limit auto-pause
- Status: DONE
- Tasks file: tasks/PR-040.md
- Branch: pr-040-rate-limit
- Depends on: PR-041

## PR-042: Async Claude CLI
- Status: DONE
- Tasks file: tasks/PR-042.md
- Branch: pr-042-async-cli
- Depends on: PR-038

## PR-043: GitHub API optimization
- Status: DONE
- Tasks file: tasks/PR-043.md
- Branch: pr-043-gh-api-optimize
- Depends on: PR-042

## PR-044: Heartbeat during Claude CLI calls
- Status: DONE
- Tasks file: tasks/PR-044.md
- Branch: pr-044-heartbeat
- Depends on: PR-042

## PR-045: FIX idle timeout based on last push
- Status: DONE
- Tasks file: tasks/PR-045.md
- Branch: pr-045-idle-timeout
- Depends on: PR-042

## PR-046: Per-repo poll interval
- Status: DONE
- Tasks file: tasks/PR-046.md
- Branch: pr-046-per-repo-poll
- Depends on: PR-042

## PR-047: Fix review status PENDING for any Codex comment after anchor
- Status: DONE
- Tasks file: tasks/PR-047.md
- Branch: pr-047-review-status-pending
- Depends on: PR-046

## PR-048: Fail-closed for GitHub observation errors
- Status: DONE
- Tasks file: tasks/PR-048.md
- Branch: pr-048-fail-closed-observation
- Depends on: PR-047

## PR-049: Refresh PR branch from origin before FIX
- Status: DONE
- Tasks file: tasks/PR-049.md
- Branch: pr-049-fix-branch-refresh
- Depends on: PR-046

## PR-050: Verify head SHA change after FIX push
- Status: DONE
- Tasks file: tasks/PR-050.md
- Branch: pr-050-fix-head-verification
- Depends on: PR-049

## PR-051: Check for existing open PR before CODING
- Status: DONE
- Tasks file: tasks/PR-051.md
- Branch: pr-051-coding-pr-guard
- Depends on: PR-046

## PR-052: Empty CI rollup returns PENDING not SUCCESS
- Status: DONE
- Tasks file: tasks/PR-052.md
- Branch: pr-052-empty-rollup-pending
- Depends on: PR-046

## PR-061: Split rate-limit thresholds into session and weekly
- Status: DONE
- Tasks file: tasks/PR-061.md
- Branch: pr-061-rate-limit-thresholds
- Depends on: PR-052

## PR-062: PAUSED state for rate limit
- Status: DONE
- Tasks file: tasks/PR-062.md
- Branch: pr-062-paused-state
- Depends on: PR-061

## PR-053: Separate timeout from rate-limit in error handler
- Status: DONE
- Tasks file: tasks/PR-053.md
- Branch: pr-053-timeout-classification
- Depends on: PR-062

## PR-054: Bounded retry/backoff for transient command failures
- Status: DONE
- Tasks file: tasks/PR-054.md
- Branch: pr-054-transient-retry
- Depends on: PR-053

## PR-055: Strict queue validation
- Status: DONE
- Tasks file: tasks/PR-055.md
- Branch: pr-055-queue-validation
- Depends on: PR-046

## PR-056: Review status parser strictness
- Status: DONE
- Tasks file: tasks/PR-056.md
- Branch: pr-056-review-status-parser
- Depends on: PR-047

## PR-058: Upload staging cleanup guards
- Status: DONE
- Tasks file: tasks/PR-058.md
- Branch: pr-058-upload-cleanup
- Depends on: PR-046

## PR-059: AGENTS.md update - artifacts and stale review
- Status: DONE
- Tasks file: tasks/PR-059.md
- Branch: pr-059-agents-update
- Depends on: PR-046

## PR-060: Settings UX - inline hints and logical grouping
- Status: DONE
- Tasks file: tasks/PR-060.md
- Branch: pr-060-settings-ux
- Depends on: PR-046

## PR-063: Proactive usage check via OAuth usage endpoint
- Status: DONE
- Tasks file: tasks/PR-063.md
- Branch: pr-063-proactive-usage-check
- Depends on: PR-062

## PR-064: In-flight rate limit monitor via statusline hook
- Status: DONE
- Tasks file: tasks/PR-064.md
- Branch: pr-064-statusline-inflight-monitor
- Depends on: PR-063

## PR-065: Codex CLI as alternative coder with manual selection
- Status: DONE
- Tasks file: tasks/PR-065.md
- Branch: pr-065-codex-coder
- Depends on: PR-063

## PR-066: Codex CLI rate limit detection
- Status: DONE
- Tasks file: tasks/PR-066.md
- Branch: pr-066-codex-rate-limit
- Depends on: PR-065, PR-063

## PR-057: Runner.py decomposition
- Status: DONE
- Tasks file: tasks/PR-057.md
- Branch: pr-057-runner-decomposition
- Depends on: PR-053, PR-054, PR-055, PR-056, PR-058, PR-059, PR-060, PR-061, PR-062, PR-063, PR-065, PR-066

## PR-067: Test hygiene - ruff and pytest-asyncio warnings
- Status: DONE
- Tasks file: tasks/PR-067.md
- Branch: pr-067-test-hygiene

## PR-068: Remove heartbeat spam from event log
- Status: DONE
- Tasks file: tasks/PR-068.md
- Branch: pr-068-heartbeat-cleanup

## PR-069: BUG-1 - no FIX when CI is PENDING
- Status: DONE
- Tasks file: tasks/PR-069.md
- Branch: pr-069-no-fix-ci-pending

## PR-070: Async run_claude in merge handler
- Status: DONE
- Tasks file: tasks/PR-070.md
- Branch: pr-070-async-merge-resolve

## PR-071: Pydantic validation for config fields
- Status: DONE
- Tasks file: tasks/PR-071.md
- Branch: pr-071-config-validation

## PR-072: BUG-3 - codex review dedup by PR number
- Status: DONE
- Tasks file: tasks/PR-072.md
- Branch: pr-072-review-dedup-by-pr

## PR-073: CoderPlugin protocol and registry
- Status: DONE
- Tasks file: tasks/PR-073.md
- Branch: pr-073-coder-plugin-protocol

## PR-074: Claude coder plugin
- Status: DONE
- Tasks file: tasks/PR-074.md
- Branch: pr-074-claude-plugin
- Depends on: PR-073

## PR-075: Codex coder plugin
- Status: DONE
- Tasks file: tasks/PR-075.md
- Branch: pr-075-codex-plugin
- Depends on: PR-073

## PR-076: Shared process-level usage provider
- Status: DONE
- Tasks file: tasks/PR-076.md
- Branch: pr-076-shared-usage-provider
- Depends on: PR-074, PR-075

## PR-077: Runner uses plugin registry
- Status: DONE
- Tasks file: tasks/PR-077.md
- Branch: pr-077-runner-plugin-integration
- Depends on: PR-074, PR-075, PR-076

## PR-078: Unified coder settings table
- Status: DONE
- Tasks file: tasks/PR-078.md
- Branch: pr-078-coder-settings-table
- Depends on: PR-077

## PR-079: Model dropdown validated from plugin
- Status: DONE
- Tasks file: tasks/PR-079.md
- Branch: pr-079-model-dropdown
- Depends on: PR-078

## PR-080: RunRecord schema and storage
- Status: DONE
- Tasks file: tasks/PR-080.md
- Branch: pr-080-run-record-schema

## PR-081: Capture metrics in coding handler
- Status: DONE
- Tasks file: tasks/PR-081.md
- Branch: pr-081-coding-metrics
- Depends on: PR-080

## PR-082: Capture metrics in fix and merge handlers
- Status: DONE
- Tasks file: tasks/PR-082.md
- Branch: pr-082-fix-merge-metrics
- Depends on: PR-081

## PR-083: Dashboard per-PR metrics panel
- Status: TODO
- Tasks file: tasks/PR-083.md
- Branch: pr-083-metrics-dashboard
- Depends on: PR-082

## PR-084: Task file header parser
- Status: TODO
- Tasks file: tasks/PR-084.md
- Branch: pr-084-task-header-parser

## PR-085: Status derivation from git
- Status: TODO
- Tasks file: tasks/PR-085.md
- Branch: pr-085-status-from-git
- Depends on: PR-084

## PR-086: DAG builder and eligible task selector
- Status: TODO
- Tasks file: tasks/PR-086.md
- Branch: pr-086-dag-builder
- Depends on: PR-085

## PR-087: Auto-queue integration in handle_idle
- Status: TODO
- Tasks file: tasks/PR-087.md
- Branch: pr-087-auto-queue-idle
- Depends on: PR-086

## PR-088: QUEUE.md auto-generation
- Status: TODO
- Tasks file: tasks/PR-088.md
- Branch: pr-088-queue-auto-generate
- Depends on: PR-087

## PR-089: Upload validation for task file headers
- Status: TODO
- Tasks file: tasks/PR-089.md
- Branch: pr-089-upload-header-validation
- Depends on: PR-084

## PR-090: Expanded error classification
- Status: TODO
- Tasks file: tasks/PR-090.md
- Branch: pr-090-error-classification

## PR-091: Auto-fallback chain on rate limit
- Status: TODO
- Tasks file: tasks/PR-091.md
- Branch: pr-091-auto-fallback
- Depends on: PR-077

## PR-092: DESIGN.md
- Status: TODO
- Tasks file: tasks/PR-092.md
- Branch: pr-092-design-doc
- Depends on: PR-077, PR-087
