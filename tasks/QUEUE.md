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
- Status: TODO
- Tasks file: tasks/PR-011.md
- Branch: pr-011-scaffolding
- Depends on: PR-009

## PR-012: Fix sprint 1 issues
- Status: TODO
- Tasks file: tasks/PR-012.md
- Branch: pr-012-sprint1-fixes
- Depends on: PR-006
