This fixture pins recovery for a task whose branch is reported merged by the
GitHub API even when the git subject scan misses the PR id. It protects the
MegaRAID-style conventional commit case that previously left stale DOING work
eligible after restart. The expected queue marks the task DONE so recovery can
settle in IDLE without relaunching completed work.
