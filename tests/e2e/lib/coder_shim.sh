#!/usr/bin/env bash
# Mock coder shim used by the e2e test stack. Replaces the real claude and
# codex CLIs inside the test containers so daemon flows can be exercised
# without making LLM API calls. The shim performs real git and gh operations
# and is driven by a SCENARIO string (see VALID_SCENARIOS in coder_shim.py).
set -euo pipefail

REPO_DIR="/data/repos/AlexBomber12__pipeline-orchestrator-testbed"

read_scenario() {
    if [[ -n "${PO_SHIM_SCENARIO_FILE:-}" && -f "${PO_SHIM_SCENARIO_FILE}" ]]; then
        head -n 1 "${PO_SHIM_SCENARIO_FILE}" | tr -d '[:space:]'
        return
    fi
    if [[ -n "${SHIM_SCENARIO:-}" ]]; then
        printf '%s' "${SHIM_SCENARIO}" | tr -d '[:space:]'
        return
    fi
    printf 'success'
}

parse_doing_task() {
    # Prints "PR-NUMBER<TAB>BRANCH_NAME" on stdout and exits 0 on success.
    # Returns non-zero when no DOING task is found.
    # Prefers the daemon's test-only runtime marker and falls back to the
    # first PR-*.md task file whose header/frontmatter says Status: DOING.
    # Status-less task files are accepted as a final fallback because the
    # dashboard upload template does not write a Status header.
    local repo_path="${1:-${REPO_DIR}}"
    local runtime_file
    runtime_file="$(_active_pr_runtime_path "${repo_path}")"
    local pr=""
    local task_file=""
    local branch=""
    if [[ -f "${runtime_file}" ]]; then
        pr="$(head -n 1 "${runtime_file}" | tr -d '[:space:]')"
        if [[ -n "${pr}" ]]; then
            task_file="${repo_path}/tasks/${pr}.md"
            if [[ -f "${task_file}" ]]; then
                branch="$(task_branch "${task_file}")"
                if [[ -n "${branch}" ]]; then
                    printf '%s\t%s\n' "${pr}" "${branch}"
                    return 0
                fi
            fi
        fi
        pr=""
        task_file=""
    fi

    local f
    local statusless_task_file=""
    for f in "${repo_path}"/tasks/PR-*.md; do
        [[ -f "${f}" ]] || continue
        if grep -Eq "^-? ?Status:[[:space:]]*DOING([[:space:]]*)$" "${f}"; then
            task_file="${f}"
            break
        fi
        if [[ -z "${statusless_task_file}" ]] &&
            ! grep -Eq "^-? ?Status:[[:space:]]*" "${f}"; then
            statusless_task_file="${f}"
        fi
    done
    if [[ -z "${task_file}" ]]; then
        task_file="${statusless_task_file}"
    fi
    if [[ -z "${task_file}" ]]; then
        return 1
    fi

    pr="$(task_pr_id "${task_file}")"
    if [[ -z "${pr}" ]]; then
        return 1
    fi
    branch="$(task_branch "${task_file}")"
    if [[ -z "${branch}" ]]; then
        return 1
    fi
    printf '%s\t%s\n' "${pr}" "${branch}"
}

task_pr_id() {
    local task_file="$1"
    awk '
        /^# PR-[A-Za-z0-9_.-]+:/ {
            match($0, /PR-[A-Za-z0-9_.-]+/)
            print substr($0, RSTART, RLENGTH)
            exit 0
        }
    ' "${task_file}"
}

task_branch() {
    local task_file="$1"
    awk '
        /^-? ?Branch:[[:space:]]*/ {
            sub(/^-? ?Branch:[[:space:]]*/, "")
            print
            exit 0
        }
    ' "${task_file}"
}

_active_pr_runtime_path() {
    local repo_path="$1"
    printf '%s/.daemon-runtime/active-pr-id\n' "${repo_path}"
}

git_setup_branch() {
    local branch="$1"
    git config user.email "shim@test.invalid"
    git config user.name "Shim Coder"
    git fetch origin
    git checkout -B "${branch}" origin/main
}

write_marker_and_commit() {
    local pr="$1"
    mkdir -p tests
    local timestamp
    timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    printf 'shim marker for %s at %s\n' "${pr}" "${timestamp}" >> tests/e2e-shim-marker.txt
    git add tests/e2e-shim-marker.txt
    git commit -m "${pr}: shim implementation"
}

ensure_pr_url() {
    # Reuse an existing open PR for the head branch when present, otherwise
    # create one. FIX FEEDBACK invocations land on a branch that already has a
    # PR from the prior CODING pass; `gh pr create` would fail in that case
    # and the daemon would record a coder failure (Codex P1).
    local branch="$1" pr="$2"
    local existing
    existing="$(gh pr list --head "${branch}" --state open --json url --jq '.[0].url' 2>/dev/null || true)"
    if [[ -n "${existing}" && "${existing}" != "null" ]]; then
        printf '%s' "${existing}"
        return
    fi
    gh pr create --base main --head "${branch}" --title "${pr}: shim" --body "Shim PR for testing"
}

safe_push_branch() {
    local branch="$1"
    # Refresh local tracking ref so the lease check below compares against
    # current remote state, not a possibly-stale cached value from an earlier
    # preserve push or fetch. The explicit refspec ensures we update
    # refs/remotes/origin/<branch> even if the default fetch refspec misses
    # branches not present locally with tracking already configured.
    git update-ref -d "refs/remotes/origin/${branch}" 2>/dev/null || true
    git fetch origin "+refs/heads/${branch}:refs/remotes/origin/${branch}" 2>/dev/null || true
    local expected
    expected="$(git rev-parse --verify "refs/remotes/origin/${branch}" 2>/dev/null || true)"
    if [ -n "${expected}" ]; then
        git push -u origin "${branch}" --force-with-lease="${branch}:${expected}"
    else
        git push -u origin "${branch}" --force-with-lease
    fi
}

run_success() {
    local pr="$1" branch="$2"
    git_setup_branch "${branch}"
    write_marker_and_commit "${pr}"
    safe_push_branch "${branch}"
    local pr_url
    pr_url="$(ensure_pr_url "${branch}" "${pr}")"
    gh pr comment "${pr_url}" --body "@codex review"
}

run_success_pending_ci() {
    # Like run_success, but also publishes a pending commit status on the
    # head SHA before exiting. The status is posted BEFORE the shim
    # returns, so the daemon's first WATCH poll observes CI=PENDING and
    # cannot race a green-with-pending-review merge before the test can
    # post REQUEST_CHANGES. Posted by the testbed App, which already
    # carries Commit statuses: Write per docs/ci-setup.md.
    local pr="$1" branch="$2"
    git_setup_branch "${branch}"
    write_marker_and_commit "${pr}"
    safe_push_branch "${branch}"
    local sha
    sha="$(git rev-parse HEAD)"
    local pr_url
    pr_url="$(ensure_pr_url "${branch}" "${pr}")"
    if ! gh api -X POST "repos/AlexBomber12/pipeline-orchestrator-testbed/statuses/${sha}" \
        -f state=pending \
        -f context=e2e/watch-merge-gate \
        -f description="e2e gate to block WATCH merge before review post" \
        >/dev/null 2>&1; then
        printf 'shim: failed to post pending status on %s; test will likely fail at REQUEST_CHANGES\n' "${sha}" >&2
    fi
    gh pr comment "${pr_url}" --body "@codex review"
}

run_no_pr() {
    local pr="$1" branch="$2"
    git_setup_branch "${branch}"
    write_marker_and_commit "${pr}"
    safe_push_branch "${branch}"
}

run_malformed_pr() {
    local pr="$1" branch="$2"
    local bad_branch
    bad_branch="$(printf '%s' "${branch}" | sed -E "s/^pr-[^-]+-/wrong-prefix-${pr}-/")"
    if [[ "${bad_branch}" == "${branch}" ]]; then
        bad_branch="wrong-prefix-${pr}"
    fi
    git_setup_branch "${bad_branch}"
    write_marker_and_commit "${pr}"
    safe_push_branch "${bad_branch}"
    local pr_url
    pr_url="$(ensure_pr_url "${bad_branch}" "${pr}")"
    gh pr comment "${pr_url}" --body "@codex review"
}

run_slow() {
    local pr="$1" branch="$2"
    git_setup_branch "${branch}"
    sleep 30
    write_marker_and_commit "${pr}"
    safe_push_branch "${branch}"
    local pr_url
    pr_url="$(ensure_pr_url "${branch}" "${pr}")"
    gh pr comment "${pr_url}" --body "@codex review"
}

run_escalate() {
    # PR-166: emit the ESCALATE marker so the daemon's FIX-cycle parser
    # transitions the runner to IDLE without further coder work.
    printf 'shim: cannot fix this in a FIX cycle\n'
    printf 'ESCALATE: e2e shim self-report\n'
}

main() {
    local invoked
    invoked="$(basename "$0")"

    # The web container mounts the same shim and probes `claude --version`,
    # `codex --version`, and `codex login status` to populate the auth panel.
    # Without a short-circuit those read-only probes would race the daemon
    # by mutating branches and creating PRs in the testbed (Codex P1).
    local arg
    for arg in "$@"; do
        case "${arg}" in
            --version|-V)
                printf '%s 0.0.0-shim\n' "${invoked}"
                exit 0
                ;;
        esac
    done
    if [[ "${invoked}" == "codex" && "${1:-}" == "login" ]]; then
        printf 'Logged in (shim)\n'
        exit 0
    fi

    # Only proceed when invoked with the daemon's coding flags. The daemon runs
    # `claude --print ...` and `codex ... exec ...`; any other invocation is a
    # no-op so non-coder probes cannot trigger git/gh side effects.
    local is_coding=0
    case "${invoked}" in
        claude)
            for arg in "$@"; do
                if [[ "${arg}" == "--print" ]]; then
                    is_coding=1
                    break
                fi
            done
            ;;
        codex)
            for arg in "$@"; do
                if [[ "${arg}" == "exec" ]]; then
                    is_coding=1
                    break
                fi
            done
            ;;
    esac
    if [[ "${is_coding}" -ne 1 ]]; then
        printf 'shim: %s invoked without coding flags, exiting 0\n' "${invoked}" >&2
        exit 0
    fi

    # ``handle_error`` invokes the coder with a fixed-shape diagnose prompt
    # asking for one of FIX / SKIP / ESCALATE. The real CLI replies; the
    # shim has no LLM so it answers SKIP, which lets the daemon clear a
    # transient ERROR (e.g. a "Base branch was modified" merge race in a
    # prior test) and return to IDLE before the next e2e test starts.
    # Without this, ERROR persists and downstream tests time out waiting
    # for IDLE.
    for arg in "$@"; do
        if [[ "${arg}" == *"FIX, SKIP, or ESCALATE"* ]]; then
            printf 'SKIP\n'
            exit 0
        fi
    done

    local scenario
    scenario="$(read_scenario)"

    if [[ "${scenario}" == "exit_nonzero" ]]; then
        printf 'shim: simulating coder failure\n' >&2
        exit 1
    fi

    if [[ "${scenario}" == "hang" ]]; then
        sleep 120
        exit 0
    fi

    if [[ "${scenario}" == "escalate" ]]; then
        # ESCALATE bypasses the testbed-repo / DOING-task plumbing: the
        # daemon only needs the marker on stdout to enter the
        # coder-initiated parking path (PR-166).
        run_escalate
        exit 0
    fi

    if [[ ! -d "${REPO_DIR}" ]]; then
        printf 'shim: testbed repo not found at %s, exiting 0\n' "${REPO_DIR}" >&2
        exit 0
    fi

    cd "${REPO_DIR}"

    local task_info
    if ! task_info="$(parse_doing_task "${REPO_DIR}")"; then
        printf 'shim: no active PR task, exiting 0\n' >&2
        exit 0
    fi
    local pr branch
    IFS=$'\t' read -r pr branch <<<"${task_info}"
    if [[ -z "${pr}" || -z "${branch}" ]]; then
        printf 'shim: no active PR task, exiting 0\n' >&2
        exit 0
    fi

    case "${scenario}" in
        success)
            run_success "${pr}" "${branch}"
            ;;
        success_pending_ci)
            run_success_pending_ci "${pr}" "${branch}"
            ;;
        no_pr)
            run_no_pr "${pr}" "${branch}"
            ;;
        malformed_pr)
            run_malformed_pr "${pr}" "${branch}"
            ;;
        slow)
            run_slow "${pr}" "${branch}"
            ;;
        escalate)
            run_escalate
            ;;
        *)
            printf 'shim: unknown scenario %s, defaulting to success\n' "${scenario}" >&2
            run_success "${pr}" "${branch}"
            ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
