This fixture covers the PR-186 crash signature: a task was active when the
daemon died, but no matching PR exists on recovery. The crashed task set is the
durable signal that the task must be canceled pending manual re-upload. The
expected queue preserves the task metadata and marks it CANCELED.
