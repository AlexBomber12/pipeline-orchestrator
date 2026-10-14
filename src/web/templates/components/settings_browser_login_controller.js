/* Dormant adapter for one Settings coder panel. The panel owns rendering and input clearing. */
(function (root) {
    "use strict";

    function createBrowserLoginController(coderName, options = {}) {
        const fetchRequest = options.fetch || root.fetch.bind(root);
        const schedule = options.setTimeout || root.setTimeout.bind(root);
        const unschedule = options.clearTimeout || root.clearTimeout.bind(root);
        const makeAbortController = options.AbortController || root.AbortController;
        const onView = options.onView || function () {};
        const onSuccess = options.onSuccess || function () {};
        const interval = options.pollInterval ?? 2000;
        const deadline = options.fetchTimeout ?? 10000;
        const base = "/api/coders/" + encodeURIComponent(coderName) + "/browser-login";
        let disposed = false;
        let generation = 0;
        let session = null;
        let timer = null;
        let state = null;
        let cleanupConfirmed = null;
        const requests = new Set();
        const submitted = new Set();
        const cancelled = new Set();
        const succeeded = new Set();
        const refusals = new Set(["unsupported", "cli_missing", "unsupported_cli_version",
            "replacement_required", "credential_in_use", "session_capacity",
            "auth_status_unavailable", "startup_failed", "daemon_shutdown", "cancelled",
            "session_not_found", "session_plugin_mismatch", "daemon_unavailable"]);
        const active = new Set(["starting", "waiting_for_user", "waiting_for_code", "authorizing", "canceling"]);
        const states = new Set([...active, "succeeded", "failed", "cancelled", "expired",
            "timed_out", "not_found", "unsupported", "cleanup_failed"]);

        function clearPolling() {
            if (timer !== null) unschedule(timer);
            timer = null;
        }

        function supersede() {
            generation += 1;
            clearPolling();
            for (const controller of requests) controller.abort();
            return generation;
        }

        function current(version) { return !disposed && version === generation; }
        function emit(view, version) { if (current(version)) onView(view); }
        function sessionUrl() {
            return base + "/" + encodeURIComponent(session.id) +
                "?reference=" + encodeURIComponent(session.reference);
        }

        async function request(url, method, body) {
            const controller = new makeAbortController();
            requests.add(controller);
            let timeout;
            try {
                return await Promise.race([
                    Promise.resolve().then(async () => {
                        const response = await fetchRequest(url, {
                        method,
                        signal: controller.signal,
                        ...(body === undefined ? {} : {
                            headers: { "Content-Type": "application/json" },
                            body: JSON.stringify(body),
                        }),
                        });
                        return response.json();
                    }),
                    new Promise((_, reject) => {
                        timeout = schedule(() => {
                            controller.abort();
                            reject(new Error("request deadline"));
                        }, deadline);
                    }),
                ]);
            } catch (_) {
                return null;
            } finally {
                if (timeout !== undefined) unschedule(timeout);
                requests.delete(controller);
            }
        }

        function classify(payload) {
            if (!payload || typeof payload !== "object") return { kind: "unavailable" };
            if (payload.ok === false && refusals.has(payload.reason) &&
                typeof payload.detail === "string") {
                return { kind: "refusal", reason: payload.reason };
            }
            const login = payload.login;
            if (payload.ok !== true || !login || typeof login !== "object" ||
                login.login_method !== "browser_code" || login.plugin !== coderName ||
                !states.has(login.state) ||
                !(login.cleanup_confirmed === null || typeof login.cleanup_confirmed === "boolean") ||
                !(login.authorization_url === null || typeof login.authorization_url === "string") ||
                !(login.application_deadline === null ||
                    typeof login.application_deadline === "number") ||
                typeof login.replacement_requested !== "boolean" ||
                typeof login.reused_session !== "boolean" ||
                !(login.auth_status === null ||
                    (typeof login.auth_status === "object" && !Array.isArray(login.auth_status))) ||
                (login.session_id !== null &&
                    (typeof login.session_id !== "string" ||
                        !/^[a-zA-Z0-9_-]{32,128}$/.test(login.session_id)))) {
                return { kind: "unavailable" };
            }
            if ((active.has(login.state) || login.state === "succeeded") && !login.session_id) {
                return { kind: "unavailable" };
            }
            if (typeof payload.reference !== "string" || !payload.reference) {
                return { kind: "unavailable" };
            }
            return { kind: "snapshot", reference: payload.reference, login };
        }

        function retryable(result) {
            return result.kind === "unavailable" ||
                (result.kind === "refusal" && result.reason === "daemon_unavailable");
        }

        function show(result, version, fromStart) {
            if (!current(version)) return;
            if (result.kind !== "snapshot") {
                emit(result.kind === "refusal" ? result : { kind: "unavailable" }, version);
                return;
            }
            const login = result.login;
            if (fromStart && login.session_id) {
                const id = login.session_id;
                session = { id, reference: result.reference };
            } else if (session && (login.session_id !== session.id ||
                result.reference !== session.reference)) {
                emit({ kind: "unavailable" }, version);
                return;
            }
            state = login.state;
            cleanupConfirmed = login.cleanup_confirmed;
            const view = {
                kind: "snapshot",
                sessionId: login.session_id,
                state: login.state,
                authorizationUrl: typeof login.authorization_url === "string" ? login.authorization_url : null,
                applicationDeadline: typeof login.application_deadline === "number" ? login.application_deadline : null,
                cleanupConfirmed: login.cleanup_confirmed === true ? true :
                    login.cleanup_confirmed === false ? false : null,
                replacementRequested: login.replacement_requested === true,
                reusedSession: login.reused_session === true,
            };
            emit(view, version);
            if (login.state === "succeeded" && login.session_id &&
                login.cleanup_confirmed === true && login.auth_status &&
                login.auth_status.status === "ok" &&
                login.auth_status.saved_credentials_present === true &&
                login.auth_status.failure_reason === null &&
                !succeeded.has(login.session_id)) {
                succeeded.add(login.session_id);
                if (current(version)) onSuccess({ sessionId: login.session_id });
            }
            if (session && login.session_id && active.has(login.state)) queuePoll(version);
        }

        function queuePoll(version) {
            if (!current(version) || timer !== null || !session) return;
            timer = schedule(() => {
                timer = null;
                if (current(version)) void inspect(version);
            }, interval);
        }

        async function inspect(version) {
            const context = session;
            if (!context || !current(version)) return;
            const result = classify(await request(sessionUrl(), "GET"));
            if (!current(version) || session !== context) return;
            if (retryable(result)) {
                emit(result, version);
                queuePoll(version);
            } else {
                show(result, version, false);
            }
        }

        async function start(replaceExisting = false) {
            if (disposed || typeof replaceExisting !== "boolean" || state === "cleanup_failed" ||
                (session && cancelled.has(session.id) && cleanupConfirmed !== true)) return false;
            const version = supersede();
            const result = classify(await request(base, "POST", { replace_existing: replaceExisting }));
            show(result, version, true);
            if (result.kind !== "snapshot" && session && active.has(state)) queuePoll(version);
            return current(version) && result.kind === "snapshot";
        }

        async function submit(authorizationCode) {
            if (disposed || !session || submitted.has(session.id) ||
                state !== "waiting_for_code" || typeof authorizationCode !== "string") return false;
            // Claim synchronously; the code is used only to build this one request body.
            submitted.add(session.id);
            const version = supersede();
            const result = classify(await request(base + "/" + encodeURIComponent(session.id) + "/code",
                "POST", { reference: session.reference, authorization_code: authorizationCode }));
            if (!current(version)) return false;
            show(result, version, false);
            if (retryable(result)) {
                queuePoll(version);
            }
            return result.kind === "snapshot";
        }

        async function cancel() {
            if (disposed || !session || cancelled.has(session.id) || state === "cleanup_failed") return false;
            cancelled.add(session.id);
            const version = supersede();
            const result = classify(await request(sessionUrl(), "DELETE"));
            if (!current(version)) return false;
            show(result, version, false);
            if (retryable(result)) {
                queuePoll(version);
            }
            return result.kind === "snapshot";
        }

        function dispose() {
            if (disposed) return;
            disposed = true;
            supersede();
            session = null;
        }

        return Object.freeze({ start, submit, cancel, dispose });
    }

    root.createBrowserLoginController = createBrowserLoginController;
})(typeof window === "undefined" ? globalThis : window);
