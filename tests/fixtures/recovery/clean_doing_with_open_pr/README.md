This fixture covers the clean restart path where a task has a matching open PR.
The helper should derive DOING directly from the open PR branch rather than
requiring QUEUE.md state. PR-266b will use this as a full recovery anchor for
resuming WATCH on the existing PR.
