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
