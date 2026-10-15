"""Execute the dormant browser-login controller against synthetic transport."""

import subprocess
from pathlib import Path

CONTROLLER = (
    Path(__file__).resolve().parents[2]
    / "src/web/templates/components/settings_browser_login_controller.js"
)


def test_browser_login_controller() -> None:
    script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[1], 'utf8');
const context = { AbortController, TextEncoder, setTimeout, clearTimeout };
vm.createContext(context);
vm.runInContext(source, context);
const make = context.createBrowserLoginController;
const tick = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => {
    let resolve;
    const promise = new Promise(r => { resolve = r; });
    return { promise, resolve };
};
const sid = 's'.repeat(32);
const sid2 = 't'.repeat(32);
const reference = 'ref /?&';
const snapshot = (state, id = sid, more = {}) => ({
    ok: true, reference,
    login: { login_method: 'browser_code', plugin: 'claude', session_id: id,
        state, cleanup_confirmed: null, authorization_url: null,
        application_deadline: 1800000000, replacement_requested: false,
        reused_session: false, auth_status: null, ...more },
});
const response = value => ({
    status: value.reason === 'daemon_unavailable' ? 503 : value.ok === false ? 409 : 200,
    json: async () => value,
});
function harness() {
    const calls = [], views = [], successes = [], timers = new Map();
    let nextTimer = 1;
    const fetch = (url, init) => {
        const pending = deferred();
        calls.push({ url, init, pending });
        return pending.promise;
    };
    const setTimeout = fn => { const id = nextTimer++; timers.set(id, fn); return id; };
    const clearTimeout = id => timers.delete(id);
    const controller = make('claude', { fetch, setTimeout, clearTimeout,
        AbortController, onView: view => views.push(view),
        onSuccess: value => successes.push(value), pollInterval: 2, fetchTimeout: 10 });
    const finish = async (index, value) => {
        calls[index].pending.resolve(response(value));
        await tick(); await tick();
    };
    const firePoll = async () => {
        const entry = [...timers.entries()].at(-1);
        assert(entry, 'poll timer exists');
        timers.delete(entry[0]); entry[1](); await tick();
    };
    return { calls, views, successes, timers, controller, finish, firePoll };
}
async function main() {
    {
        const h = harness();
        const started = h.controller.start(true);
        const duplicateStart = h.controller.start(true);
        assert.equal(await duplicateStart, false);
        await tick();
        assert.equal(h.calls.length, 1);
        assert.equal(h.controller.actions().start, false);
        assert.equal(h.calls[0].init.method, 'POST');
        assert.deepEqual(JSON.parse(h.calls[0].init.body), { replace_existing: true });
        await h.finish(0, snapshot('waiting_for_code'));
        assert.equal(await started, true);
        assert.equal(h.controller.actions().submit, true);
        for (const invalid of ['   ', 'secret\nline', 'x'.repeat(4097),
            'é'.repeat(2049), '\ud800']) {
            assert.equal(await h.controller.submit(invalid), false);
            assert.equal(h.calls.length, 1);
        }
        const code = 'secret+exact/ value 🔒';
        const first = h.controller.submit(code);
        const second = h.controller.submit(code);
        assert.equal(await second, false);
        assert.equal(h.controller.actions().submit, false);
        await tick();
        assert.equal(h.calls.length, 2);
        assert.equal(h.calls[1].url, '/api/coders/claude/browser-login/' + sid + '/code');
        assert.deepEqual(JSON.parse(h.calls[1].init.body),
            { reference, authorization_code: code });
        await h.finish(1, { ok: false, reason: 'daemon_unavailable', detail: code });
        assert.equal(await first, false);
        assert.equal(h.views.at(-1).reason, 'daemon_unavailable');
        assert.equal(h.views.at(-1).actions.start, false);
        assert.equal(h.timers.size, 1); // inspect the captured session after lost handoff
        assert.equal(JSON.stringify(h.views).includes(code), false);
        assert.equal(JSON.stringify(h.controller).includes(code), false);
        assert.equal(await h.controller.submit(code), false);
        const reused = h.controller.start(false);
        await tick(); await h.finish(2, snapshot('waiting_for_code', sid, { reused_session: true }));
        await reused;
        assert.equal(await h.controller.submit(code), false);
        await h.firePoll();
        assert.equal(h.calls[3].init.method, 'GET');
        assert.equal(h.calls[3].url,
            '/api/coders/claude/browser-login/' + sid + '?reference=ref%20%2F%3F%26');
        assert.equal([...h.timers.values()].length, 1); // fetch deadline only
        await h.finish(3, { ok: false, reason: 'daemon_unavailable', detail: 'unavailable' });
        assert.equal(h.timers.size, 1);
        await h.firePoll();
        assert.equal(h.calls[4].init.method, 'GET');
        await h.finish(4, snapshot('succeeded', sid, { cleanup_confirmed: true,
            auth_status: { status: 'ok', saved_credentials_present: true,
                failure_reason: null } }));
        assert.equal(h.successes.length, 1);
        assert.equal(h.views.at(-1).actions.cancel, false);
        assert.equal(await h.controller.cancel(), false);
        assert.equal(h.calls.length, 5);
        assert.equal(h.timers.size, 0);
        const again = h.controller.start(false);
        await tick(); await h.finish(5, snapshot('succeeded', sid, { cleanup_confirmed: true,
            auth_status: { status: 'ok', saved_credentials_present: true,
                failure_reason: null } }));
        await again;
        assert.equal(h.successes.length, 1);
        const different = h.controller.start(false);
        await tick(); await h.finish(6, snapshot('waiting_for_code', sid2));
        await different;
        const newSubmit = h.controller.submit('another');
        await tick(); assert.equal(h.calls[7].init.method, 'POST');
        await h.finish(7, snapshot('waiting_for_code', sid2));
        await newSubmit;
        h.controller.dispose();
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('waiting_for_code')); await start;
        const cancel = h.controller.cancel(); await tick();
        assert.equal(h.calls[1].init.method, 'DELETE');
        assert.equal(h.calls[1].url,
            '/api/coders/claude/browser-login/' + sid + '?reference=ref%20%2F%3F%26');
        await h.finish(1, { nonsense: true }); await cancel;
        assert.equal(await h.controller.cancel(), false);
        await h.firePoll();
        assert.equal(h.calls[2].init.method, 'GET');
        await h.finish(2, snapshot('cleanup_failed', sid, { cleanup_confirmed: false }));
        assert.equal(h.timers.size, 0);
        assert.equal(await h.controller.start(), false);
        assert.equal(h.successes.length, 0);
        const retry = h.controller.cancel(); await tick();
        assert.equal(h.calls[3].init.method, 'DELETE');
        assert.equal(await h.controller.cancel(), false);
        await h.finish(3, snapshot('cancelled', sid, { cleanup_confirmed: true }));
        assert.equal(await retry, true);
        assert.equal(h.controller.actions().cancel, false);
        const restart = h.controller.start(); await tick();
        await h.finish(4, snapshot('waiting_for_code', sid2));
        assert.equal(await restart, true);
        h.controller.dispose();
    }
    for (const outage of ['503', 'timeout']) {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('waiting_for_code')); await start;
        const cancel = h.controller.cancel(); await tick();
        assert.equal(await h.controller.cancel(), false);
        if (outage === '503') {
            await h.finish(1, { ok: false, reason: 'daemon_unavailable', detail: 'unavailable' });
        } else {
            [...h.timers.values()][0]();
        }
        assert.equal(await cancel, false);
        assert.equal(h.calls[1].init.method, 'DELETE');
        if (outage === 'timeout') assert.equal(h.calls[1].init.signal.aborted, true);
        assert.equal(await h.controller.cancel(), false);
        await h.firePoll();
        assert.equal(h.calls[2].init.method, 'GET');
        await h.finish(2, snapshot('waiting_for_code'));
        assert.equal(h.views.at(-1).actions.cancelRetry, true);
        assert.equal(h.views.at(-1).actions.submit, false);
        assert.equal(h.calls.length, 3); // polling never repeats a mutation
        assert.equal(await h.controller.submit('must-not-send'), false);
        const retry = h.controller.cancel(); await tick();
        assert.equal(h.calls[3].init.method, 'DELETE');
        assert.equal(h.calls[3].url, h.calls[1].url);
        assert.equal(await h.controller.cancel(), false);
        assert.equal(await h.controller.submit('must-not-send'), false);
        assert.equal(h.calls.length, 4);
        await h.finish(3, snapshot('cancelled', sid, { cleanup_confirmed: true }));
        assert.equal(await retry, true);
        h.controller.dispose();
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('waiting_for_code')); await start;
        await h.firePoll();
        await h.finish(1, { ok: false, reason: 'session_not_found', detail: 'gone' });
        assert.equal(h.views.at(-1).actions.start, true);
        assert.equal(h.views.at(-1).actions.submit, false);
        assert.equal(h.views.at(-1).actions.cancel, false);
        assert.equal(await h.controller.submit('must-not-send'), false);
        assert.equal(h.calls.length, 2);
        const restart = h.controller.start(); await tick();
        await h.finish(2, { ok: false, reason: 'daemon_unavailable', detail: 'offline' });
        assert.equal(await restart, false);
        assert.equal(h.timers.size, 0); // no polling of the known-missing ID
        assert.equal(await h.controller.submit('still-missing'), false);
        assert.equal(h.calls.length, 3);
        const newStart = h.controller.start(); await tick();
        await h.finish(3, snapshot('waiting_for_code', sid2));
        assert.equal(await newStart, true);
        assert.equal(h.controller.actions().submit, true);
        const newSubmit = h.controller.submit('new-session-code'); await tick();
        assert.equal(h.calls[4].url,
            '/api/coders/claude/browser-login/' + sid2 + '/code');
        await h.finish(4, snapshot('authorizing', sid2));
        assert.equal(await newSubmit, true);
        h.controller.dispose();
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('waiting_for_code')); await start;
        const cancel = h.controller.cancel(); await tick();
        await h.finish(1, snapshot('cleanup_failed', sid, { cleanup_confirmed: false }));
        await cancel;
        assert.equal(await h.controller.start(), false);
        assert.equal(h.views.at(-1).actions.cancelRetry, true);
        assert.equal(h.views.at(-1).actions.cancel, true);
        const retry = h.controller.cancel(); await tick();
        await h.finish(2, { ok: false, reason: 'session_not_found', detail: 'gone' });
        assert.equal(await retry, false);
        const restart = h.controller.start(); await tick();
        await h.finish(3, snapshot('waiting_for_code', sid2));
        assert.equal(await restart, true);
        h.controller.dispose();
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('waiting_for_code')); await start;
        await h.firePoll();
        assert.equal(h.calls.length, 2);
        assert.equal(h.timers.size, 1); // only fetch deadline while the read is pending
        const pending = h.calls[1];
        const newer = h.controller.start(); await tick();
        await h.finish(2, snapshot('waiting_for_code', sid2)); await newer;
        pending.pending.resolve(response(snapshot('succeeded', sid, { cleanup_confirmed: true,
            auth_status: { status: 'ok', saved_credentials_present: true,
                failure_reason: null } })));
        await tick(); await tick();
        assert.equal(h.successes.length, 0);
        assert.equal(h.views.at(-1).sessionId, sid2);
        h.controller.dispose();
        assert.equal(h.timers.size, 0);
        const count = h.views.length;
        assert.equal(await h.controller.start(), false);
        assert.equal(h.views.length, count);
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        const deadline = [...h.timers.values()][0]; deadline();
        assert.equal(await start, false);
        assert.equal(h.calls[0].init.signal.aborted, true);
        assert.equal(h.views.at(-1).kind, 'unavailable');
        assert.equal(h.timers.size, 0);
        h.controller.dispose();
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('waiting_for_code')); await start;
        const cancel = h.controller.cancel(); await tick();
        assert.equal(await h.controller.submit('must-not-send'), false);
        assert.equal(h.calls.length, 2);
        await h.finish(1, { ok: false, reason: 'session_not_found', detail: 'gone' });
        assert.equal(await cancel, false);
        const restart = h.controller.start(); await tick();
        assert.equal(h.calls[2].init.method, 'POST');
        await h.finish(2, snapshot('waiting_for_code', sid2));
        assert.equal(await restart, true);
        h.controller.dispose();
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('waiting_for_code')); await start;
        const reattach = h.controller.start(); await tick();
        await h.finish(1, { ok: false, reason: 'daemon_unavailable', detail: 'unavailable' });
        assert.equal(await reattach, false);
        assert.equal(h.timers.size, 1);
        const refused = h.controller.start(true); await tick();
        await h.finish(2, { ok: false, reason: 'credential_in_use', detail: 'in use' });
        assert.equal(await refused, false);
        assert.equal(h.timers.size, 1);
        await h.firePoll();
        assert.equal(h.calls[3].init.method, 'GET');
        await h.finish(3, snapshot('cancelled', sid, { cleanup_confirmed: true }));
        assert.equal(h.views.at(-1).state, 'cancelled');
        assert.equal(h.timers.size, 0);
        h.controller.dispose();
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('waiting_for_code')); await start;
        const cancel = h.controller.cancel(); await tick();
        await h.finish(1, { ok: false, reason: 'daemon_unavailable', detail: 'unavailable' });
        assert.equal(await cancel, false);
        assert.equal(h.timers.size, 1);
        await h.firePoll();
        await h.finish(2, snapshot('succeeded', sid, { cleanup_confirmed: true,
            auth_status: { status: 'ok', saved_credentials_present: true,
                failure_reason: null } }));
        assert.equal(h.successes.length, 1);
        const restart = h.controller.start(); await tick();
        assert.equal(h.calls[3].init.method, 'POST');
        await h.finish(3, snapshot('waiting_for_code', sid2));
        assert.equal(await restart, true);
        h.controller.dispose();
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('waiting_for_code')); await start;
        const submit = h.controller.submit('private-code'); await tick();
        h.controller.dispose();
        h.calls[1].pending.resolve(response(snapshot('succeeded', sid, {
            cleanup_confirmed: true,
            auth_status: { status: 'ok', saved_credentials_present: true,
                failure_reason: null },
        })));
        await submit;
        assert.equal(h.successes.length, 0);
        assert.equal(h.views.length, 1);
        assert.equal(h.calls[1].init.signal.aborted, true);
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, { ok: false, reason: 'replacement_required', detail: 'warning' });
        assert.equal(await start, false);
        assert.equal(h.views.at(-1).reason, 'replacement_required');
        assert.equal(h.calls.length, 1);
        const replace = h.controller.start(true);
        assert.equal(await h.controller.start(true), false);
        await tick();
        assert.equal(h.calls.length, 2);
        assert.deepEqual(JSON.parse(h.calls[1].init.body), { replace_existing: true });
        await h.finish(1, snapshot('waiting_for_code'));
        assert.equal(await replace, true);
        h.controller.dispose();
    }
    for (const result of [
        { ok: false, reason: 'unsupported_cli_version', detail: 'upgrade required' },
        { ok: false, reason: 'cli_missing', detail: 'CLI unavailable' },
        snapshot('unsupported', null),
    ]) {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, result); await start;
        assert.equal(h.views.at(-1).actions.start, false);
        assert.equal(await h.controller.start(), false);
        assert.equal(h.calls.length, 1);
        h.controller.dispose();
    }
    {
        const h = harness();
        const start = h.controller.start(); await tick();
        await h.finish(0, snapshot('succeeded', sid, { cleanup_confirmed: true,
            auth_status: { status: 'ok', saved_credentials_present: true,
                failure_reason: null } }));
        await start;
        const unsupported = h.controller.start(); await tick();
        await h.finish(1, snapshot('unsupported', null));
        assert.equal(await unsupported, true);
        assert.equal(h.views.at(-1).state, 'unsupported');
        assert.equal(h.views.at(-1).actions.start, false);
        assert.equal(await h.controller.start(), false);
        assert.equal(h.calls.length, 2);
        h.controller.dispose();
    }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
"""
    result = subprocess.run(
        ["node", "-e", script, str(CONTROLLER)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_browser_login_panel_adapter() -> None:
    script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[1], 'utf8');
const tick = () => new Promise(resolve => setImmediate(resolve));
const sid = 's'.repeat(32);
const reference = 'ref';
const snapshot = (state, more = {}) => ({ ok: true, reference, login: {
    login_method: 'browser_code', plugin: 'claude', session_id: sid, state,
    cleanup_confirmed: null, authorization_url: 'https://example.test/authorize',
    application_deadline: 1800000000, replacement_requested: false,
    reused_session: false, auth_status: null, ...more,
} });
class Element {
    constructor() {
        this.dataset = {}; this.value = ''; this.textContent = '';
        this.isConnected = true; this.listeners = {};
        this.hidden = new Set(['hidden']);
        this.classList = { toggle: (name, enabled) => enabled
            ? this.hidden.add(name) : this.hidden.delete(name) };
    }
    addEventListener(type, fn) { this.listeners[type] = fn; }
    fire(type) { this.listeners[type]({ preventDefault() {} }); }
    removeAttribute(name) { delete this[name]; }
}
const selectors = ['status', 'expiry', 'link', 'form', 'code', 'replace',
    'start', 'cancel', 'refresh'];
const nodes = Object.fromEntries(selectors.map(name => [name, new Element()]));
nodes.confirm = new Element();
const panel = new Element();
panel.id = 'coder-claude-browser-login';
panel.dataset.browserLoginPlugin = 'claude';
panel.querySelector = selector => {
    if (selector.includes('action="start"')) return nodes.start;
    if (selector.includes('action="cancel"')) return nodes.cancel;
    if (selector.includes('action="replace"')) return nodes.confirm;
    return nodes[selector.match(/data-browser-login-([a-z]+)/)[1]];
};
const events = [], calls = [], timers = new Map();
let timerId = 0;
const context = {
    AbortController, TextEncoder, URL, CustomEvent: class {
        constructor(type, options) { this.type = type; this.detail = options.detail; }
    },
    location: { pathname: '/settings' },
    document: {
        querySelectorAll: () => panel.isConnected ? [panel] : [],
        body: { dispatchEvent: event => events.push(event) },
    },
    fetch: (url, init) => new Promise(resolve => calls.push({ url, init, resolve })),
    setTimeout: fn => { const id = ++timerId; timers.set(id, fn); return id; },
    clearTimeout: id => timers.delete(id),
};
vm.createContext(context);
vm.runInContext(source, context);
const finish = async (index, payload) => {
    calls[index].resolve({ json: async () => payload });
    await tick(); await tick();
};
async function main() {
    context.reconcileBrowserLoginPanels();
    context.reconcileBrowserLoginPanels();
    assert.equal(calls.length, 0); // rendering never starts a login
    nodes.start.fire('click');
    nodes.start.fire('click');
    await tick();
    assert.equal(calls.length, 1); // one panel has one controller/listener
    await finish(0, snapshot('waiting_for_code'));
    assert.equal(nodes.link.href, 'https://example.test/authorize');
    assert.equal(nodes.form.hidden.has('hidden'), false);
    nodes.code.value = 'private-code';
    nodes.form.fire('submit');
    assert.equal(nodes.code.value, ''); // cleared before fetch runs
    nodes.form.fire('submit');
    await tick();
    assert.equal(calls.length, 2);
    assert.equal(JSON.parse(calls[1].init.body).authorization_code, 'private-code');
    await finish(1, { ok: false, reason: 'daemon_unavailable', detail: 'private-code' });
    assert.equal(nodes.form.hidden.has('hidden'), true);
    assert.equal(nodes.start.hidden.has('hidden'), true);
    const poll = [...timers.entries()].at(-1);
    timers.delete(poll[0]); poll[1](); await tick();
    await finish(2, snapshot('waiting_for_code'));
    assert.equal(nodes.form.hidden.has('hidden'), true); // ambiguous submit is one-shot
    nodes.cancel.fire('click'); await tick();
    await finish(3, { ok: false, reason: 'daemon_unavailable', detail: 'unknown' });
    const recovered = [...timers.entries()].at(-1);
    timers.delete(recovered[0]); recovered[1](); await tick();
    await finish(4, snapshot('waiting_for_code'));
    assert.equal(nodes.cancel.textContent, 'Retry cancellation');
    nodes.cancel.fire('click'); await tick();
    assert.equal(calls[5].init.method, 'DELETE');
    await finish(5, snapshot('cleanup_failed', { cleanup_confirmed: false }));
    assert.equal(nodes.cancel.textContent, 'Retry cancellation');
    assert.match(nodes.status.textContent, /unconfirmed/);
    nodes.cancel.fire('click'); await tick();
    await finish(6, snapshot('cancelled', { cleanup_confirmed: true }));
    nodes.start.fire('click'); await tick();
    await finish(7, snapshot('succeeded', { cleanup_confirmed: true,
        auth_status: { status: 'ok', saved_credentials_present: true,
            failure_reason: null } }));
    assert.equal(events.length, 1);
    assert.equal(events[0].detail.shellId, panel.id);
    const previousRefreshToken = events[0].detail.refreshToken;
    assert.equal(nodes.cancel.hidden.has('hidden'), true);
    assert.equal(nodes.refresh.hidden.has('hidden'), false);
    nodes.start.fire('click');
    assert.equal(nodes.refresh.textContent, '');
    assert.equal(nodes.refresh.hidden.has('hidden'), true);
    assert.notEqual(nodes.refresh.dataset.loginRefreshToken, previousRefreshToken);
    await tick();
    await finish(8, snapshot('unsupported', null));
    assert.equal(nodes.start.hidden.has('hidden'), true);
    context.reconcileBrowserLoginPanels();
    assert.equal(calls.length, 9);
    panel.isConnected = false;
    context.reconcileBrowserLoginPanels();
    assert.equal(nodes.code.value, '');
    panel.isConnected = true;
    context.reconcileBrowserLoginPanels();
    nodes.start.fire('click'); await tick();
    await finish(9, snapshot('waiting_for_code'));
    const pending = [...timers.entries()].at(-1);
    timers.delete(pending[0]); pending[1](); await tick();
    panel.isConnected = false;
    context.reconcileBrowserLoginPanels();
    await finish(10, snapshot('succeeded', { cleanup_confirmed: true,
        auth_status: { status: 'ok', saved_credentials_present: true,
            failure_reason: null } }));
    assert.equal(events.length, 1); // disposed panel suppresses late success
    panel.isConnected = true;
    context.reconcileBrowserLoginPanels();
    nodes.start.fire('click'); await tick();
    await finish(11, { ok: false, reason: 'unsupported_cli_version', detail: 'upgrade' });
    assert.equal(nodes.start.hidden.has('hidden'), true);
    nodes.start.fire('click'); await tick();
    assert.equal(calls.length, 12);
    panel.isConnected = false;
    context.reconcileBrowserLoginPanels();
    panel.isConnected = true;
    context.reconcileBrowserLoginPanels();
    nodes.start.fire('click'); await tick();
    await finish(12, snapshot('waiting_for_code'));
    const missingPoll = [...timers.entries()].at(-1);
    timers.delete(missingPoll[0]); missingPoll[1](); await tick();
    nodes.code.value = 'must-be-cleared';
    await finish(13, { ok: false, reason: 'session_not_found', detail: 'gone' });
    assert.equal(nodes.start.hidden.has('hidden'), false);
    assert.equal(nodes.cancel.hidden.has('hidden'), true);
    assert.equal(nodes.form.hidden.has('hidden'), true);
    assert.equal(nodes.code.value, '');
    assert.match(nodes.status.textContent, /previous session is gone/);
    nodes.code.value = 'must-not-send';
    nodes.form.fire('submit'); await tick();
    assert.equal(calls.length, 14);
    assert.equal(nodes.code.value, '');
    nodes.start.fire('click'); await tick();
    assert.equal(calls[14].init.method, 'POST');
    await finish(14, { ok: false, reason: 'daemon_unavailable', detail: 'offline' });
    assert.equal(timers.size, 0);
    nodes.start.fire('click'); await tick();
    await finish(15, snapshot('waiting_for_code', { session_id: 't'.repeat(32) }));
    assert.equal(nodes.form.hidden.has('hidden'), false);
    nodes.code.value = 'new-session-code';
    nodes.form.fire('submit'); await tick();
    assert.equal(calls[16].url,
        '/api/coders/claude/browser-login/' + 't'.repeat(32) + '/code');
    assert.equal(JSON.parse(calls[16].init.body).authorization_code, 'new-session-code');
    await finish(16, snapshot('authorizing', { session_id: 't'.repeat(32) }));
}
main().catch(error => { console.error(error); process.exitCode = 1; });
"""
    result = subprocess.run(
        ["node", "-e", script, str(CONTROLLER)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
