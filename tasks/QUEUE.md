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

## PR-027: Subprocess helper and squash merge
- Status: TODO
- Tasks file: tasks/PR-027.md
- Branch: pr-027-subprocess-helper
- Depends on: PR-026

## PR-028: QUEUE.md auto-update on merge
- Status: TODO
- Tasks file: tasks/PR-028.md
- Branch: pr-028-queue-auto-done
- Depends on: PR-027

## PR-029: Deduplicate codex review posts
- Status: TODO
- Tasks file: tasks/PR-029.md
- Branch: pr-029-dedup-codex-review
- Depends on: PR-028

## PR-030: Detect and push unpushed commits
- Status: TODO
- Tasks file: tasks/PR-030.md
- Branch: pr-030-unpushed-detect
- Depends on: PR-027

## PR-031: Pre-merge sync with main
- Status: TODO
- Tasks file: tasks/PR-031.md
- Branch: pr-031-pre-merge-sync
- Depends on: PR-027

## PR-032: Claude CLI stdout logging
- Status: TODO
- Tasks file: tasks/PR-032.md
- Branch: pr-032-cli-stdout-log
- Depends on: PR-027

## PR-033: Model config in config.yml
- Status: TODO
- Tasks file: tasks/PR-033.md
- Branch: pr-033-model-config
- Depends on: PR-032

## PR-034: Event log UX overhaul
- Status: TODO
- Tasks file: tasks/PR-034.md
- Branch: pr-034-event-log-ux
- Depends on: PR-026

## PR-035: Repo card click and dashboard polish
- Status: TODO
- Tasks file: tasks/PR-035.md
- Branch: pr-035-card-click
- Depends on: PR-034
