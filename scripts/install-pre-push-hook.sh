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
# Validates the local refs being pushed against the expected-branch
# marker when the daemon has written it. Manual git operations (no
# expected-branch file) are no-op.
#
# Per githooks(5), pre-push receives ``<local-ref> <local-oid>
# <remote-ref> <remote-oid>`` lines on stdin. The hook validates BOTH
# the local source and the remote destination on each line: a push
# like ``git push origin pr-001:main`` carries a matching local ref
# but routes commits to ``main`` on origin, which would bypass the
# branch-safety gate if only the local side were checked. Using stdin
# rather than ``git symbolic-ref --short HEAD`` is necessary because
# the push refspec can differ from the checked-out branch: ``git push
# origin main`` while HEAD is on the expected feature branch would
# silently pass a HEAD-only check, and pushing the expected branch
# from a detached/other checkout would falsely trip one. The marker
# path is resolved via ``git rev-parse --git-path info/expected-branch``
# so linked worktrees (where ``.git`` is a file pointing at
# ``<main-repo>/.git/worktrees/<name>/``) and repos initialized with
# ``--separate-git-dir`` find the per-checkout marker; a hardcoded
# ``.git/info/expected-branch`` lookup would silently no-op there.
set -euo pipefail

if ! EXPECTED_FILE=$(git rev-parse --git-path info/expected-branch 2>/dev/null); then
    exit 0
fi

if [[ ! -f "$EXPECTED_FILE" ]]; then
    exit 0
fi

EXPECTED=$(<"$EXPECTED_FILE")

while read -r local_ref local_sha remote_ref remote_sha; do
    case "$remote_ref" in
        refs/heads/*) actual_remote="${remote_ref#refs/heads/}" ;;
        *) actual_remote="$remote_ref" ;;
    esac
    # ``local-ref`` is the literal string ``(delete)`` for ref
    # deletions; nothing is pushed from the local side, but the
    # remote destination is still part of the push contract and
    # must be validated. Without this check, ``git push origin
    # --delete main`` would bypass the gate and remove a
    # protected base branch even when a task branch is the only
    # ref the daemon expects to be touched.
    if [[ "$local_ref" == "(delete)" ]]; then
        if [[ "$EXPECTED" != "$actual_remote" ]]; then
            echo "[pre-push-hook] BLOCKED: expected branch '$EXPECTED' but push deletes '$actual_remote' (remote ref '$remote_ref'). Aborting push." >&2
            exit 1
        fi
        continue
    fi
    # ``local-ref`` is ``HEAD`` for explicit HEAD refspec pushes such as
    # ``git push origin HEAD:<branch>``; HEAD is a symbolic alias resolved
    # by git to the current commit and carries no branch identity of its
    # own, so the local-side equality check would falsely reject every
    # such push. The remote-ref check below is the security-critical
    # validation (it gates the destination branch on origin), so skipping
    # the local check on HEAD does not weaken the gate.
    case "$local_ref" in
        refs/heads/*) actual_local="${local_ref#refs/heads/}" ;;
        HEAD) actual_local="" ;;
        *) actual_local="$local_ref" ;;
    esac
    if [[ -n "$actual_local" ]] && [[ "$EXPECTED" != "$actual_local" ]]; then
        echo "[pre-push-hook] BLOCKED: expected branch '$EXPECTED' but push includes '$actual_local' (local ref '$local_ref'). Aborting push." >&2
        exit 1
    fi
    if [[ "$EXPECTED" != "$actual_remote" ]]; then
        echo "[pre-push-hook] BLOCKED: expected branch '$EXPECTED' but push targets '$actual_remote' (remote ref '$remote_ref'). Aborting push." >&2
        exit 1
    fi
done

exit 0
EOF

chmod +x "$HOOK_FILE"
