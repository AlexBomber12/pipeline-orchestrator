# Task file schema (`tasks/PR-*.md`)

Every PR has a corresponding task file under `tasks/`. The header at the
top of the file is parsed by `src/queue_parser.py:parse_task_header` and
must contain the following fields:

```
# PR-XXX: Short title

Branch: <branch-name>
- Type: <type>
- Complexity: low | medium | high
- Depends on: none | PR-001, PR-002, ...
- Priority: 1-5
- Coder: claude | codex | any
```

## Type field

Canonical values:

- `architecture`
- `bugfix`
- `config`
- `docs`
- `feature`
- `refactor`
- `ux`

### Type field synonyms

The parser also accepts a small number of common synonyms. They are
normalized to the canonical value at parse time but are otherwise
equivalent. Synonyms are accepted but discouraged in new task files;
scripts and AI assistants generating task files should output canonical
values directly.

| Input synonym | Canonical value |
| ------------- | ---------------- |
| `bug`         | `bugfix`         |
| `fix`         | `bugfix`         |
| `chore`       | `refactor`       |
| `feat`        | `feature`        |
| `task`        | `feature`        |

The synonym map is one-way and immutable: once a synonym is shipped, its
key must never be reused for a different canonical value, since doing so
would silently change the meaning of historical task files.

## Validation

Validation runs at upload time (dashboard) and on the daemon's IDLE
selector. Unknown values for any field cause the task to be rejected.
For batch uploads, validation collects errors across all files and
returns one aggregated report instead of stopping at the first failure.
