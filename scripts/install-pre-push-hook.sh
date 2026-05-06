#!/bin/bash
# Installs the pipeline-orchestrator pre-push branch-validation hook in $1
# (repo path). The hook reads .git/info/expected-branch (written by the
# daemon's CODING handler) and aborts the push if HEAD is on a different
# branch than the daemon expected. When the file is absent the hook is a
# no-op so manual operator git operations are not affected.
#
# Idempotent: overwrites any existing pre-push hook unconditionally so
# rerunning on every scaffolder pass self-heals a deleted or corrupted
# hook file.
set -euo pipefail

REPO="${1:?usage: $0 <repo_path>}"
HOOKS_DIR="$REPO/.git/hooks"
HOOK_FILE="$HOOKS_DIR/pre-push"

mkdir -p "$HOOKS_DIR"

cat > "$HOOK_FILE" <<'EOF'
#!/bin/bash
# Pipeline-orchestrator pre-push hook (installed by scaffolder).
# Validates the local branch name against .git/info/expected-branch
# when the daemon has written it. Manual git operations (no
# expected-branch file) are no-op.
set -euo pipefail

EXPECTED_FILE=".git/info/expected-branch"
if [[ ! -f "$EXPECTED_FILE" ]]; then
    exit 0
fi

EXPECTED=$(<"$EXPECTED_FILE")
ACTUAL=$(git symbolic-ref --short HEAD 2>/dev/null || echo "<detached>")

if [[ "$EXPECTED" != "$ACTUAL" ]]; then
    echo "[pre-push-hook] BLOCKED: expected branch '$EXPECTED' but HEAD is on '$ACTUAL'. Aborting push." >&2
    exit 1
fi

exit 0
EOF

chmod +x "$HOOK_FILE"
