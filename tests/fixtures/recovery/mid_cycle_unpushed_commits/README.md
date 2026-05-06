This fixture covers a mid-cycle crash after local work was produced but before
the daemon completed the PR handoff. The recovery layer preserves commits before
marking the task crashed; the header helper captures the resulting queue
projection. The expected task is CANCELED so the daemon will not loop the same
crashing task without a manual re-upload.
