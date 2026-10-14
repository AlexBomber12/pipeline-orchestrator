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
const context = { AbortController, setTimeout, clearTimeout };
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
const response = value => ({ status: value.ok === false ? 409 : 200, json: async () => value });
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
        await tick();
        assert.equal(h.calls[0].init.method, 'POST');
        assert.deepEqual(JSON.parse(h.calls[0].init.body), { replace_existing: true });
        await h.finish(0, snapshot('waiting_for_code'));
        assert.equal(await started, true);
        const code = 'secret+exact/\nvalue';
        const first = h.controller.submit(code);
        const second = h.controller.submit(code);
        assert.equal(await second, false);
        await tick();
        assert.equal(h.calls.length, 2);
        assert.equal(h.calls[1].url, '/api/coders/claude/browser-login/' + sid + '/code');
        assert.deepEqual(JSON.parse(h.calls[1].init.body),
            { reference, authorization_code: code });
        await h.finish(1, { ok: false, reason: 'daemon_unavailable', detail: code });
        assert.equal(await first, false);
        assert.equal(h.views.at(-1).reason, 'daemon_unavailable');
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
        const restart = h.controller.start(); await tick();
        await h.finish(4, snapshot('waiting_for_code', sid2));
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
