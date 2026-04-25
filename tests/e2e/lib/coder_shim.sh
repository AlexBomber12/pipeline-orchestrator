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
    # Looks for the first "## PR-XXX: ..." block followed (within 4 lines) by
    # both "- Status: DOING" and "- Branch: <name>".
    local queue_file="${REPO_DIR}/tasks/QUEUE.md"
    if [[ ! -f "${queue_file}" ]]; then
        return 1
    fi
    local result
    result="$(awk '
        /^## PR-[A-Za-z0-9-]+:/ {
            match($0, /PR-[A-Za-z0-9-]+/)
            current = substr($0, RSTART, RLENGTH)
            status = ""
            branch = ""
            for (i = 1; i <= 4; i++) {
                if ((getline line) <= 0) break
                if (line ~ /^- Status: /) {
                    status = line
                    sub(/^- Status: */, "", status)
                }
                if (line ~ /^- Branch: /) {
                    branch = line
                    sub(/^- Branch: */, "", branch)
                }
            }
            if (status == "DOING" && branch != "") {
                printf "%s\t%s\n", current, branch
                exit 0
            }
        }
    ' "${queue_file}")"
    if [[ -z "${result}" ]]; then
        return 1
    fi
    printf '%s\n' "${result}"
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

run_success() {
    local pr="$1" branch="$2"
    git_setup_branch "${branch}"
    write_marker_and_commit "${pr}"
    git push -u origin "${branch}" --force-with-lease
    local pr_url
    pr_url="$(gh pr create --base main --head "${branch}" --title "${pr}: shim" --body "Shim PR for testing")"
    gh pr comment "${pr_url}" --body "@codex review"
}

run_no_pr() {
    local pr="$1" branch="$2"
    git_setup_branch "${branch}"
    write_marker_and_commit "${pr}"
    git push -u origin "${branch}" --force-with-lease
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
    git push -u origin "${bad_branch}" --force-with-lease
    local pr_url
    pr_url="$(gh pr create --base main --head "${bad_branch}" --title "${pr}: shim" --body "Shim PR for testing")"
    gh pr comment "${pr_url}" --body "@codex review"
}

run_slow() {
    local pr="$1" branch="$2"
    git_setup_branch "${branch}"
    sleep 30
    write_marker_and_commit "${pr}"
    git push -u origin "${branch}" --force-with-lease
    local pr_url
    pr_url="$(gh pr create --base main --head "${branch}" --title "${pr}: shim" --body "Shim PR for testing")"
    gh pr comment "${pr_url}" --body "@codex review"
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

    if [[ ! -d "${REPO_DIR}" ]]; then
        printf 'shim: testbed repo not found at %s, exiting 0\n' "${REPO_DIR}" >&2
        exit 0
    fi

    cd "${REPO_DIR}"

    local task_info
    if ! task_info="$(parse_doing_task)"; then
        printf 'shim: no DOING task in QUEUE.md, exiting 0\n' >&2
        exit 0
    fi
    local pr branch
    IFS=$'\t' read -r pr branch <<<"${task_info}"
    if [[ -z "${pr}" || -z "${branch}" ]]; then
        printf 'shim: no DOING task in QUEUE.md, exiting 0\n' >&2
        exit 0
    fi

    case "${scenario}" in
        success)
            run_success "${pr}" "${branch}"
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
        *)
            printf 'shim: unknown scenario %s, defaulting to success\n' "${scenario}" >&2
            run_success "${pr}" "${branch}"
            ;;
    esac
}

main "$@"
