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
        let unsupported = false;
        const requests = new Set();
        const submitted = new Set();
        const cancelled = new Set();
        const cancelInFlight = new Set();
        const cancelRetryReady = new Set();
        const missing = new Set();
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
        function actions() {
            const id = session && session.id;
            return Object.freeze({
                start: !disposed && !unsupported && !(id && active.has(state)) &&
                    !(state === "cleanup_failed" &&
                    (!id || !missing.has(id))) && !(id && cancelled.has(id) &&
                    cleanupConfirmed !== true && !missing.has(id)),
                submit: !disposed && Boolean(id) && state === "waiting_for_code" &&
                    !submitted.has(id) && !cancelled.has(id),
                cancel: !disposed && Boolean(id) &&
                    (active.has(state) || state === "cleanup_failed") &&
                    !cancelInFlight.has(id) &&
                    (!cancelled.has(id) || cancelRetryReady.has(id)),
                cancelRetry: Boolean(id) && (cancelRetryReady.has(id) ||
                    state === "cleanup_failed"),
            });
        }
        function emit(view, version) {
            if (current(version)) onView({ ...view, actions: actions() });
        }
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

        function validCode(value) {
            return typeof value === "string" && Boolean(value.trim()) &&
                !/[\p{Cc}\p{Cs}\p{Zl}\p{Zp}]/u.test(value) &&
                new TextEncoder().encode(value).length <= 4096;
        }

        function confirmedSuccess(login) {
            return login.state === "succeeded" && login.session_id &&
                login.cleanup_confirmed === true && login.auth_status &&
                login.auth_status.status === "ok" &&
                login.auth_status.saved_credentials_present === true &&
                login.auth_status.failure_reason === null;
        }

        function show(result, version, fromStart) {
            if (!current(version)) return;
            if (result.kind !== "snapshot") {
                if (result.kind === "refusal" && ["unsupported", "cli_missing",
                    "unsupported_cli_version"].includes(result.reason)) unsupported = true;
                if (!fromStart && session && result.kind === "refusal" &&
                    result.reason === "session_not_found") {
                    missing.add(session.id);
                    cancelRetryReady.delete(session.id);
                }
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
            if (login.state === "unsupported") unsupported = true;
            cleanupConfirmed = login.cleanup_confirmed;
            if (session && !active.has(login.state)) cancelRetryReady.delete(session.id);
            if (session && login.state === "cleanup_failed") cancelled.delete(session.id);
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
                successConfirmed: Boolean(confirmedSuccess(login)),
            };
            emit(view, version);
            if (confirmedSuccess(login) &&
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
                if (current(version) && session === context && result.kind === "snapshot" &&
                    result.login.session_id === context.id && result.reference === context.reference &&
                    active.has(result.login.state) && cancelled.has(context.id) &&
                    !cancelInFlight.has(context.id)) cancelRetryReady.add(context.id);
                show(result, version, false);
            }
        }

        async function start(replaceExisting = false) {
            if (disposed || unsupported || typeof replaceExisting !== "boolean" ||
                (state === "cleanup_failed" && (!session || !missing.has(session.id))) ||
                (session && cancelled.has(session.id) && cleanupConfirmed !== true &&
                    !missing.has(session.id))) return false;
            const version = supersede();
            const result = classify(await request(base, "POST", { replace_existing: replaceExisting }));
            show(result, version, true);
            if (result.kind !== "snapshot" && session && active.has(state)) queuePoll(version);
            return current(version) && result.kind === "snapshot";
        }

        async function submit(authorizationCode) {
            if (disposed || !session || submitted.has(session.id) || cancelled.has(session.id) ||
                state !== "waiting_for_code" || !validCode(authorizationCode)) return false;
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
            if (!actions().cancel) return false;
            const id = session.id;
            cancelled.add(id);
            cancelRetryReady.delete(id);
            cancelInFlight.add(id);
            const version = supersede();
            try {
                const result = classify(await request(sessionUrl(), "DELETE"));
                if (!current(version)) return false;
                cancelInFlight.delete(id);
                show(result, version, false);
                if (retryable(result)) {
                    queuePoll(version);
                }
                return result.kind === "snapshot";
            } finally {
                cancelInFlight.delete(id);
            }
        }

        function dispose() {
            if (disposed) return;
            disposed = true;
            supersede();
            session = null;
        }

        return Object.freeze({ start, submit, cancel, dispose, actions });
    }

    root.createBrowserLoginController = createBrowserLoginController;
})(typeof window === "undefined" ? globalThis : window);

/* Bind the controller to Settings panels without owning its login lifecycle. */
(function (root) {
    "use strict";
    const panels = new Map();
    const labels = {
        starting: "Starting browser login…",
        waiting_for_user: "Open the authorization page to continue.",
        waiting_for_code: "Enter the authorization code from the browser.",
        authorizing: "Checking the authorization code…",
        canceling: "Cancellation is in progress; waiting for cleanup confirmation.",
        succeeded: "Login succeeded.",
        failed: "Login failed. You can start another login.",
        cancelled: "Login cancelled; cleanup confirmed.",
        expired: "Login expired. You can start another login.",
        timed_out: "Login timed out. Check cleanup before starting again.",
        not_found: "The session was not found. You can start another login.",
        unsupported: "Browser login is unavailable. Reload Settings after updating the CLI to check capabilities again.",
        cleanup_failed: "Cleanup is unconfirmed. Retry cancellation explicitly.",
    };
    const active = new Set(["starting", "waiting_for_user", "waiting_for_code",
        "authorizing", "canceling"]);

    function setVisible(element, visible) {
        element.classList.toggle("hidden", !visible);
    }

    function mount(panel) {
        if (panels.has(panel)) return;
        const plugin = panel.dataset.browserLoginPlugin;
        if (!/^[a-z0-9][a-z0-9_-]{0,63}$/.test(plugin)) return;
        const status = panel.querySelector("[data-browser-login-status]");
        const expiry = panel.querySelector("[data-browser-login-expiry]");
        const link = panel.querySelector("[data-browser-login-link]");
        const form = panel.querySelector("[data-browser-login-form]");
        const input = panel.querySelector("[data-browser-login-code]");
        const replace = panel.querySelector("[data-browser-login-replace]");
        const start = panel.querySelector('[data-browser-login-action="start"]');
        const cancel = panel.querySelector('[data-browser-login-action="cancel"]');
        const refresh = panel.querySelector("[data-browser-login-refresh-status]");
        let alive = true;
        let refreshGeneration = 0;
        const controller = root.createBrowserLoginController(plugin, {
            onView(view) {
                if (!alive || !panel.isConnected) return;
                const snapshot = view.kind === "snapshot";
                status.textContent = snapshot ?
                    view.state === "succeeded" && !view.successConfirmed
                        ? "Authorization finished, but credential confirmation or cleanup is unconfirmed."
                        : view.state === "cancelled" && view.cleanupConfirmed !== true
                            ? "Cancellation was reported; cleanup is unconfirmed."
                            : view.cleanupConfirmed === false && view.state !== "cleanup_failed"
                                ? labels[view.state] + " Cleanup is unconfirmed."
                                : labels[view.state] :
                    view.kind === "refusal" && view.reason === "replacement_required"
                        ? "Existing credentials require your consent before replacement."
                        : view.kind === "refusal" && ["unsupported", "cli_missing",
                            "unsupported_cli_version"].includes(view.reason)
                            ? "Browser login is unavailable. Reload Settings after updating the CLI to check capabilities again."
                        : view.kind === "unavailable" || view.reason === "daemon_unavailable"
                            ? "Login status is unavailable. If a session exists, Settings will keep checking it."
                            : "Browser login request was refused (" + view.reason + ").";
                setVisible(replace, view.kind === "refusal" &&
                    view.reason === "replacement_required" && view.actions.start);
                let safeUrl = null;
                if (snapshot && view.authorizationUrl) {
                    try {
                        const parsed = new URL(view.authorizationUrl);
                        if (parsed.protocol === "https:") safeUrl = parsed.href;
                    } catch (_) { /* Invalid authorization link stays hidden. */ }
                }
                if (safeUrl) link.href = safeUrl;
                else link.removeAttribute("href");
                setVisible(link, Boolean(safeUrl) && active.has(view.state));
                setVisible(form, view.actions.submit);
                if (!view.actions.submit) input.value = "";
                const deadline = snapshot && Number.isFinite(view.applicationDeadline)
                    ? new Date(view.applicationDeadline * 1000) : null;
                expiry.textContent = deadline && !Number.isNaN(deadline.getTime()) &&
                    active.has(view.state) ? "Expires " + deadline.toLocaleString() : "";
                setVisible(expiry, Boolean(expiry.textContent));
                setVisible(start, view.actions.start &&
                    !(view.kind === "refusal" && view.reason === "replacement_required"));
                setVisible(cancel, view.actions.cancel);
                cancel.textContent = view.actions.cancelRetry ? "Retry cancellation" : "Cancel";
            },
            onSuccess() {
                if (!alive || !panel.isConnected) return;
                refreshGeneration += 1;
                refresh.dataset.loginRefreshToken = String(refreshGeneration);
                refresh.textContent = "Refreshing authentication status and model metadata…";
                setVisible(refresh, true);
                document.body.dispatchEvent(new CustomEvent("deviceLoginSucceeded", {
                    detail: { plugin, shellId: panel.id,
                        refreshSelector: "[data-browser-login-refresh-status]",
                        refreshToken: refresh.dataset.loginRefreshToken },
                }));
            },
        });
        function begin(replaceExisting) {
            refreshGeneration += 1;
            refresh.dataset.loginRefreshToken = String(refreshGeneration);
            refresh.textContent = "";
            setVisible(refresh, false);
            void controller.start(replaceExisting);
        }
        start.addEventListener("click", () => { begin(false); });
        panel.querySelector('[data-browser-login-action="replace"]').addEventListener(
            "click", () => { begin(true); });
        cancel.addEventListener("click", () => {
            input.value = "";
            void controller.cancel();
            cancel.disabled = true;
            Promise.resolve().then(() => { cancel.disabled = false; });
        });
        form.addEventListener("submit", event => {
            event.preventDefault();
            const code = input.value;
            input.value = "";
            if (controller.actions().submit) void controller.submit(code);
        });
        panels.set(panel, { controller, input, dispose() {
            alive = false;
            input.value = "";
            controller.dispose();
        } });
    }

    root.reconcileBrowserLoginPanels = function () {
        for (const [panel, entry] of panels) {
            if (!panel.isConnected || root.location.pathname !== "/settings") {
                entry.dispose();
                panels.delete(panel);
            }
        }
        if (root.location.pathname === "/settings") {
            document.querySelectorAll("[data-browser-login-panel]").forEach(mount);
        }
    };
    root.disposeBrowserLoginPanels = function () {
        for (const entry of panels.values()) entry.dispose();
        panels.clear();
    };
})(typeof window === "undefined" ? globalThis : window);
