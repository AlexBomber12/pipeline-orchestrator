(function () {
    if (window.settingsSaveFeedbackInstalled) return;
    window.settingsSaveFeedbackInstalled = true;

    const configFields = new Set([
        'guardrail_notification_webhook_url',
        'guardrail_notification_min_tier',
        'guardrail_notification_timeout_seconds',
        'dashboard_base_url',
        'spend_ceiling_session_percent',
        'spend_ceiling_weekly_percent',
        'spend_ceiling_warning_percent',
        'git_bundle_backup_enabled',
        'git_bundle_backup_daily_retention',
        'main_commit_audit_interval_idle_cycles',
    ]);
    const daemonFields = new Set([
        'rate_limit_session_pause_percent',
        'rate_limit_weekly_pause_percent',
    ]);
    const selector = '#settings-notifications [hx-swap="none"], ' +
        '#settings-usage-limits [hx-swap="none"], ' +
        '#settings-sandbox-backup [hx-swap="none"]';
    const states = new WeakMap();

    function eligible(elt) {
        if (!elt || !elt.matches || !elt.matches(selector) ||
            !elt.matches('input[name], select[name]')) return false;
        const name = elt.name;
        return (configFields.has(name) &&
            elt.getAttribute('hx-post') === '/settings/config/' + name) ||
            (daemonFields.has(name) && elt.getAttribute('hx-put') === '/settings/daemon');
    }

    function currentValue(elt) {
        return elt.type === 'checkbox' ? elt.checked : elt.value;
    }

    function setup(elt) {
        if (!eligible(elt)) return null;
        let state = states.get(elt);
        if (state) return state;
        // hx-request is the supported HTMX per-element XHR timeout setting.
        elt.setAttribute('hx-request', '{"timeout":10000}');
        const feedback = document.createElement('span');
        feedback.className = 'hidden text-xs text-gray-500';
        feedback.setAttribute('role', 'status');
        feedback.setAttribute('aria-live', 'polite');
        const message = document.createElement('span');
        const retry = document.createElement('button');
        retry.type = 'button';
        retry.className = 'hidden ml-2 text-accent underline';
        retry.textContent = 'Retry save';
        feedback.append(message, retry);
        elt.insertAdjacentElement('afterend', feedback);
        state = { feedback, message, retry, version: 0, active: null };
        states.set(elt, state);
        retry.addEventListener('click', function (event) {
            event.preventDefault();
            event.stopPropagation();
            if (state.active || !elt.isConnected || elt.disabled) return;
            htmx.trigger(elt, 'change');
        });
        return state;
    }

    function display(state, message, color, canRetry) {
        state.message.textContent = message;
        state.feedback.classList.remove('hidden', 'text-gray-500', 'text-ok', 'text-warn', 'text-fail');
        state.feedback.classList.add(color);
        state.retry.classList.toggle('hidden', !canRetry);
    }

    function source(event) {
        const elt = event.detail && event.detail.elt;
        return eligible(elt) ? elt : null;
    }

    function finish(event, transportFailure) {
        const elt = source(event);
        if (!elt) return;
        const state = states.get(elt);
        const request = state && state.active;
        if (!request || request.xhr !== event.detail.xhr || request.finished) return;
        request.finished = true;
        state.active = null;
        if (!elt.isConnected) return;
        if (state.version !== request.version || currentValue(elt) !== request.value) {
            display(state, 'Unsaved changes', 'text-warn', true);
            return;
        }
        const status = event.detail.xhr && event.detail.xhr.status;
        if (transportFailure || !status) {
            display(state, 'Save not confirmed', 'text-warn', true);
        } else if (status >= 200 && status < 300) {
            display(state, 'Saved', 'text-ok', false);
        } else {
            display(state, 'Not saved', 'text-fail', true);
        }
    }

    function scan(root) {
        if (eligible(root)) setup(root);
        if (root && root.querySelectorAll) root.querySelectorAll(selector).forEach(setup);
    }

    scan(document.body);
    document.body.addEventListener('htmx:afterSwap', function () {
        scan(document.body);
    });
    // Capture edits before HTMX's control-level change listener starts a request.
    ['input', 'change'].forEach(function (type) {
        document.body.addEventListener(type, function (event) {
            const elt = event.target;
            if (!eligible(elt)) return;
            const state = setup(elt);
            state.version += 1;
            display(state, 'Unsaved changes', 'text-warn', false);
        }, true);
    });
    document.body.addEventListener('htmx:beforeRequest', function (event) {
        const elt = source(event);
        if (!elt || !event.detail.xhr) return;
        const state = setup(elt);
        state.active = {
            xhr: event.detail.xhr,
            value: currentValue(elt),
            version: state.version,
            finished: false,
        };
        display(state, 'Saving', 'text-warn', false);
    });
    document.body.addEventListener('htmx:afterRequest', function (event) {
        finish(event, false);
    });
    document.body.addEventListener('htmx:sendError', function (event) {
        finish(event, true);
    });
    document.body.addEventListener('htmx:timeout', function (event) {
        finish(event, true);
    });
})();
