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
#
# Honors ``core.hooksPath``: git does not always read hooks from
# ``.git/hooks``. Repos (or global config) that set ``core.hooksPath``
# — including ``/dev/null`` to disable hooks entirely — would silently
# bypass an installer that hardcodes ``.git/hooks``. We ask git for the
# effective path via ``rev-parse --git-path hooks/pre-push`` so the
# installed hook lands where git will actually invoke it. When the
# configured directory is unwritable (e.g. ``core.hooksPath=/dev/null``)
# we surface a warning and exit non-zero rather than installing into a
# location that will never run.
set -euo pipefail

REPO="${1:?usage: $0 <repo_path>}"

if ! HOOK_FILE=$(git -C "$REPO" rev-parse --git-path hooks/pre-push 2>/dev/null); then
    echo "[install-pre-push-hook] git rev-parse failed for '$REPO' (not a git repo?)" >&2
    exit 1
fi

case "$HOOK_FILE" in
    /*) ;;
    *) HOOK_FILE="$REPO/$HOOK_FILE" ;;
esac

HOOKS_DIR=$(dirname "$HOOK_FILE")

if ! mkdir -p "$HOOKS_DIR" 2>/dev/null; then
    echo "[install-pre-push-hook] cannot create hooks dir '$HOOKS_DIR' (core.hooksPath disables hooks?); skipping install" >&2
    exit 1
fi

cat > "$HOOK_FILE" <<'EOF'
#!/bin/bash
# Pipeline-orchestrator pre-push hook (installed by scaffolder).
# Validates the local branch name against the expected-branch marker
# when the daemon has written it. Manual git operations (no
# expected-branch file) are no-op.
#
# Resolves the marker path via ``git rev-parse --git-path
# info/expected-branch`` so linked worktrees (where ``.git`` is a
# file pointing at ``<main-repo>/.git/worktrees/<name>/``) and repos
# initialized with ``--separate-git-dir`` find the file in the
# per-checkout ``info/`` directory git uses; a hardcoded
# ``.git/info/expected-branch`` lookup would silently no-op in those
# layouts because the path does not resolve to a regular file.
set -euo pipefail

if ! EXPECTED_FILE=$(git rev-parse --git-path info/expected-branch 2>/dev/null); then
    exit 0
fi

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
