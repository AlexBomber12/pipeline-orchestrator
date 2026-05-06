This fixture protects the PR-247 operator recovery path. A task that the
operator explicitly recovered from HUNG remains parked even if its branch still
has an open PR. The expected queue marks it CANCELED so the daemon stays IDLE
until the operator re-uploads the task.
