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
    # If neither signal exists, fail fast instead of guessing from statusless
    # uploaded task files.
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
    for f in "${repo_path}"/tasks/PR-*.md; do
        [[ -f "${f}" ]] || continue
        if task_status_is_doing "${f}"; then
            task_file="${f}"
            break
        fi
    done
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

task_status_is_doing() {
    local task_file="$1"
    awk '
        /^##[[:space:]]/ { exit 1 }
        /^-? ?Status:[[:space:]]*DOING[[:space:]]*$/ {
            found = 1
            exit 0
        }
        END {
            if (!found) {
                exit 1
            }
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

wait_for_base_activity() {
    # GitHub may record activity after git push returns. Sleeping from the
    # local push alone can still put creation and the next push in one server
    # timestamp. Observe the base activity first, then separate the next push.
    local branch="$1" sha count poll
    sha="$(git rev-parse HEAD)"
    for ((poll=0; poll<30; poll++)); do
        count="$(gh api --method GET repos/AlexBomber12/pipeline-orchestrator-testbed/activity \
            -f "ref=refs/heads/${branch}" -f per_page=100 \
            --jq "[.[] | select(.after == \"${sha}\" and (.activity_type == \"branch_creation\" or .activity_type == \"push\" or .activity_type == \"force_push\"))] | length" \
            2>/dev/null || true)"
        if [[ "${count}" =~ ^[1-9][0-9]*$ ]]; then
            sleep 2
            return 0
        fi
        sleep 1
    done
    printf 'shim: base publication activity unavailable for %s at %s\n' "${branch}" "${sha}" >&2
    return 1
}

run_success() {
    local pr="$1" branch="$2"
    git_setup_branch "${branch}"
    # Publish the base first so branch creation and the implementation push
    # cannot share GitHub's second-resolution activity timestamp. An ambiguous
    # publication correctly blocks WATCH even with allow_merge_without_review.
    safe_push_branch "${branch}"
    wait_for_base_activity "${branch}"
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
    safe_push_branch "${branch}"
    wait_for_base_activity "${branch}"
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

run_fix_existing_pr() {
    # Preserve the original PR and replace this task's marker in place.
    local pr="$1" branch="$2" pending="${3:-false}"
    git fetch origin
    git checkout -B "${branch}" "origin/${branch}"
    sed -i "\|^shim marker for ${pr} at |d" tests/e2e-shim-marker.txt
    # Make the FIX marker distinct even on a fast second-resolution clock.
    sleep 2
    write_marker_and_commit "${pr}"
    safe_push_branch "${branch}"
    if [[ "${pending}" == true ]]; then
        gh api -X POST "repos/AlexBomber12/pipeline-orchestrator-testbed/statuses/$(git rev-parse HEAD)" \
            -f state=pending -f context=e2e/watch-merge-gate >/dev/null
    fi
    local pr_url
    pr_url="$(ensure_pr_url "${branch}" "${pr}")"
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

run_crash_before_publish() {
    # Signal an actual CLI launch before creating any task ref or commit.
    # If the test fails to kill this owned process, fail without publishing.
    printf '%s\t%s\n' "$1" "$2" > "${PO_SHIM_SCENARIO_FILE}.started"
    sleep 120
    return 1
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

    # The web and daemon containers mount the same shim and probe
    # `claude --version`, `claude auth status`, `codex --version`, and
    # `codex login status` to populate auth state and select eligible coders.
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
    if [[ "${invoked}" == "claude" && "${1:-}" == "auth" && "${2:-}" == "status" ]]; then
        printf '{"loggedIn":true,"authMethod":"claude.ai"}\n'
        exit 0
    fi
    if [[ "${invoked}" == "codex" && "${1:-}" == "login" ]]; then
        printf 'Logged in using ChatGPT\n'
        exit 0
    fi

    # Synthetic catalogue over the real SDK discovery transport; no inference.
    if [[ "${invoked}" == "claude" && " $* " == *" --input-format stream-json "* ]]; then
        python3 -c '
import json, sys
request = json.loads(sys.stdin.readline())
models = [{"value": value, "displayName": value, "supportsEffort": True,
           "supportedEffortLevels": ["low", "high"]}
          for value in ("opus", "sonnet")]
print(json.dumps({"type": "control_response", "response": {
    "subtype": "success", "request_id": request["request_id"],
    "response": {"models": models}}}), flush=True)
'
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
    # shim has no LLM so it answers SKIP. The daemon retains the failed
    # outcome and releases execution only after its safety proof succeeds.
    for arg in "$@"; do
        if [[ "${arg}" == *"FIX, SKIP, or ESCALATE"* ]]; then
            printf 'SKIP\n'
            exit 0
        fi
    done

    local scenario
    scenario="$(read_scenario)"

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

    # Only the restart fixture owns this journal; never record prompts.
    if [[ "${branch}" == *-e2e-fix-restart ]]; then
        printf '%s\t%s\n' "${pr}" "${branch}" >> "${PO_SHIM_SCENARIO_FILE}.restart-calls"
    fi

    if [[ "${scenario}" == "hang" ]]; then
        # Record only allowlisted launch arguments, never prompts or credentials.
        # Atomic replacement lets the host correlate a unique task with a real
        # CLI launch independently of the daemon's pre-launch snapshot.
        if [[ "${branch}" == *-e2e-settings-first || "${branch}" == *-e2e-settings-next ]]; then
            python3 - "${PO_SHIM_SCENARIO_FILE}.settings-started" "${pr}" "${branch}" "${invoked}" "$@" <<'PYTHON'
import json, os, sys
path, task, branch, plugin, *args = sys.argv[1:]
def flag(name):
    return args[args.index(name) + 1] if name in args else None
record = {"pr_id": task, "branch": branch, "plugin_id": plugin,
          "model": flag("--model"), "effort": flag("--effort")}
with open(path + ".tmp", "w") as output:
    json.dump(record, output)
os.replace(path + ".tmp", path)
PYTHON
        fi
        sleep 120
        exit 0
    fi

    case "${scenario}" in
        failure_continuity)
            # This scenario owns a task invocation journal outside the checkout.
            # Diagnosis probes returned above; only real task attempts count.
            printf '%s\t%s\n' "${pr}" "${branch}" >> "${PO_SHIM_SCENARIO_FILE}.calls"
            case "${branch}" in
                *-clean-root|*-dirty-root)
                    git_setup_branch "${branch}"
                    if [[ "${branch}" == *-dirty-root ]]; then
                        printf 'unpreserved work for %s\n' "${pr}" > continuity-dirty.txt
                    fi
                    printf 'shim: deterministic continuity failure\n' >&2
                    exit 1
                    ;;
                *) run_success "${pr}" "${branch}" ;;
            esac
            ;;
        coding_capacity_once)
            # Fixture-owned journal outside the checkout survives the failed CLI.
            printf '%s\t%s\n' "${pr}" "${branch}" >> "${PO_SHIM_SCENARIO_FILE}.capacity-calls"
            if [[ "$(wc -l < "${PO_SHIM_SCENARIO_FILE}.capacity-calls")" -eq 1 ]]; then
                git_setup_branch "${branch}"
                mkdir -p tests
                printf 'retained commit for %s\n' "${pr}" > tests/e2e-capacity-commit.txt
                git add tests/e2e-capacity-commit.txt
                git commit -m "${pr}: retain pre-capacity work"
                printf 'retained dirty work for %s\n' "${pr}" > tests/e2e-capacity-retained.txt
                printf 'unfinished\n' > "${PO_SHIM_SCENARIO_FILE}.capacity-gate"
                printf 'ERROR: Selected model is at capacity. Please try a different model.\n' >&2
                exit 1
            fi
            # Never call git_setup_branch here: recovery must supply the same
            # committed and untracked work, rather than recreate the task.
            [[ "$(git branch --show-current)" == "${branch}" ]]
            [[ "$(git show HEAD:tests/e2e-capacity-commit.txt)" == "retained commit for ${pr}" ]]
            [[ "$(cat tests/e2e-capacity-retained.txt)" == "retained dirty work for ${pr}" ]]
            [[ "$(cat "${PO_SHIM_SCENARIO_FILE}.capacity-gate")" == unfinished ]]
            git push -u origin "${branch}"
            wait_for_base_activity "${branch}"
            git add tests/e2e-capacity-retained.txt
            write_marker_and_commit "${pr}"
            printf 'completed-after-capacity\n' > "${PO_SHIM_SCENARIO_FILE}.capacity-gate"
            git push -u origin "${branch}"
            pr_url="$(ensure_pr_url "${branch}" "${pr}")"
            gh api -X POST "repos/AlexBomber12/pipeline-orchestrator-testbed/statuses/$(git rev-parse HEAD)" \
                -f state=pending -f context=e2e/watch-merge-gate >/dev/null
            gh pr comment "${pr_url}" --body "@codex review"
            ;;
        exit_nonzero)
            # Fail with bound, clean work already preserved on the remote base.
            # A failure before branch creation deliberately lacks release proof.
            git_setup_branch "${branch}"
            printf 'shim: simulating coder failure\n' >&2
            exit 1
            ;;
        success)
            run_success "${pr}" "${branch}"
            ;;
        success_pending_ci)
            run_success_pending_ci "${pr}" "${branch}"
            ;;
        fix_existing_pr)
            run_fix_existing_pr "${pr}" "${branch}"
            ;;
        fix_published_wait)
            run_fix_existing_pr "${pr}" "${branch}" true
            # A real Git push and pending CI precede the crash handshake.
            printf '%s\t%s\t%s\n' "${pr}" "${branch}" "$(git rev-parse HEAD)" \
                > "${PO_SHIM_SCENARIO_FILE}.fix-published"
            sleep 300
            exit 1
            ;;
        fix_existing_pr_pending_ci)
            run_fix_existing_pr "${pr}" "${branch}" true
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
        crash_before_publish)
            run_crash_before_publish "${pr}" "${branch}"
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
