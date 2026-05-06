This fixture keeps legacy task files from blocking structured recovery. One
PR-*.md file has only the old free-form title and body, while the rest have
complete headers. The expected queue contains only the valid structured tasks,
in priority and PR-id order.
