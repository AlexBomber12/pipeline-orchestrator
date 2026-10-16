"""Exercise the Settings save helper with synthetic HTMX and DOM events."""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "src/web/templates/components/settings_save_feedback.js"


def test_settings_save_feedback() -> None:
    script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

class Element {
    constructor(section = '', name = '', method = '', path = '', swap = '') {
        this.section = section;
        this.name = name;
        this.attrs = { ['hx-' + method]: path, 'hx-swap': swap };
        this.type = 'text';
        this.value = '';
        this.checked = false;
        this.isConnected = true;
        this.disabled = false;
        this.children = [];
        this.listeners = {};
        this.classList = {
            values: new Set(),
            add: (...items) => items.forEach(x => this.classList.values.add(x)),
            remove: (...items) => items.forEach(x => this.classList.values.delete(x)),
            toggle: (item, force) => {
                if (force) this.classList.values.add(item);
                else this.classList.values.delete(item);
            },
            contains: item => this.classList.values.has(item),
        };
    }
    matches(selector) {
        if (selector === 'input[name], select[name]') return !!this.name;
        return selector.split(',').some(part =>
            part.includes('#' + this.section + ' ') && this.attrs['hx-swap'] === 'none');
    }
    getAttribute(name) { return this.attrs[name] ?? null; }
    setAttribute(name, value) { this.attrs[name] = value; }
    append(...items) { this.children.push(...items); }
    insertAdjacentElement(_position, item) { this.feedback = item; }
    addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }
    click() {
        const event = { preventDefault() {}, stopPropagation() {} };
        for (const fn of this.listeners.click || []) fn(event);
    }
}

const controls = [];
const body = {
    listeners: {},
    querySelectorAll() { return controls; },
    addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); },
    fire(type, detail = {}, target = null) {
        const event = { type, detail, target };
        for (const fn of this.listeners[type] || []) fn(event);
    },
};
const document = { body, createElement: () => new Element() };
const triggered = [];
const htmx = { trigger: (elt, type) => triggered.push([elt, type]) };
const window = {};
const context = { document, window, htmx };
vm.createContext(context);
const source = fs.readFileSync(process.argv[1], 'utf8');
const config = (section, name) => new Element(section, name, 'post',
    '/settings/config/' + name, 'none');
const daemon = name => new Element('settings-usage-limits', name, 'put',
    '/settings/daemon', 'none');
const webhook = config('settings-notifications', 'guardrail_notification_webhook_url');
const tier = config('settings-notifications', 'guardrail_notification_min_tier');
const session = daemon('rate_limit_session_pause_percent');
const backup = config('settings-sandbox-backup', 'git_bundle_backup_enabled');
backup.type = 'checkbox';
const excluded = new Element('settings-notifications', 'other', 'post',
    '/settings/webhook/test', 'none');
const reset = new Element('settings-usage-limits', 'spend_ceiling_session_percent',
    'post', '/settings/config/reset/spend_ceiling', 'none');
const otherPage = config('other-page', 'dashboard_base_url');
controls.push(webhook, tier, session, backup, excluded, reset, otherPage);
const remaining = [
    ...['guardrail_notification_timeout_seconds', 'dashboard_base_url'].map(
        name => config('settings-notifications', name)),
    ...['spend_ceiling_session_percent', 'spend_ceiling_weekly_percent',
        'spend_ceiling_warning_percent'].map(
        name => config('settings-usage-limits', name)),
    ...['git_bundle_backup_daily_retention',
        'main_commit_audit_interval_idle_cycles'].map(
        name => config('settings-sandbox-backup', name)),
    daemon('rate_limit_weekly_pause_percent'),
];
controls.push(...remaining);
vm.runInContext(source, context);
const initialListeners = Object.fromEntries(Object.entries(body.listeners).map(
    ([name, entries]) => [name, entries.length]));
vm.runInContext(source, context); // repeated inclusion must not register twice
assert.deepEqual(Object.fromEntries(Object.entries(body.listeners).map(
    ([name, entries]) => [name, entries.length])), initialListeners);
for (const elt of [webhook, tier, session, backup, ...remaining]) {
    assert.equal(elt.getAttribute('hx-request'), '{"timeout":10000}');
    assert.equal(elt.feedback.getAttribute('role'), 'status');
    assert.equal(elt.feedback.getAttribute('aria-live'), 'polite');
}
for (const elt of [excluded, reset, otherPage]) assert.equal(elt.feedback, undefined);
const message = elt => elt.feedback.children[0].textContent;
const retry = elt => elt.feedback.children[1];
const start = (elt, xhr) => body.fire('htmx:beforeRequest', { elt, xhr });
const done = (elt, xhr, type = 'htmx:afterRequest') => body.fire(type, { elt, xhr });
const edit = (elt, value, type = 'input') => {
    if (elt.type === 'checkbox') elt.checked = value;
    else elt.value = value;
    body.fire(type, {}, elt);
};

edit(webhook, 'https://secret.example/token');
assert.equal(message(webhook), 'Unsaved changes');
const success = { status: 200 };
start(webhook, success);
assert.equal(message(webhook), 'Saving');
done(webhook, success);
assert.equal(message(webhook), 'Saved');
assert.equal(retry(webhook).classList.contains('hidden'), true);

for (const status of [400, 422, 503]) {
    edit(tier, String(status), 'change');
    const xhr = { status };
    start(tier, xhr);
    done(tier, xhr);
    assert.equal(message(tier), 'Not saved');
    assert.equal(retry(tier).classList.contains('hidden'), false);
}

for (const event of ['htmx:timeout', 'htmx:sendError']) {
    edit(session, event);
    const xhr = { status: 0 };
    start(session, xhr);
    done(session, xhr, event);
    done(session, xhr); // HTMX may send both error and afterRequest
    assert.equal(message(session), 'Save not confirmed');
    assert.equal(triggered.length, 0); // no automatic write retry
    retry(session).click();
    assert.deepEqual(triggered.at(-1), [session, 'change']);
    triggered.length = 0;
}

for (const field of [session, remaining.at(-1)]) {
    edit(field, '');
    const ignoredBlank = { status: 200 };
    start(field, ignoredBlank);
    done(field, ignoredBlank);
    assert.equal(message(field), 'Not saved');
    assert.equal(retry(field).classList.contains('hidden'), true);
}

edit(webhook, 'first');
const old = { status: 200 };
start(webhook, old);
edit(webhook, 'second');
assert.equal(message(webhook), 'Unsaved changes');
done(webhook, old);
assert.equal(message(webhook), 'Unsaved changes');
assert.equal(webhook.value, 'second');
assert.equal(retry(webhook).classList.contains('hidden'), false);
retry(webhook).click();
assert.deepEqual(triggered.at(-1), [webhook, 'change']);
triggered.length = 0;
const newer = { status: 200 };
start(webhook, newer);
done(webhook, old); // late duplicate completion cannot replace the new request
assert.equal(message(webhook), 'Saving');
done(webhook, newer);
assert.equal(message(webhook), 'Saved');

edit(backup, true, 'change');
const backupXhr = { status: 200 };
start(backup, backupXhr);
edit(backup, false, 'change');
done(backup, backupXhr);
assert.equal(backup.checked, false);
assert.equal(message(backup), 'Unsaved changes');

const swapped = config('settings-sandbox-backup', 'git_bundle_backup_daily_retention');
controls.push(swapped);
body.fire('htmx:afterSwap');
assert.equal(swapped.getAttribute('hx-request'), '{"timeout":10000}');
assert(swapped.feedback);
const ignored = { status: 200 };
start(excluded, ignored); done(excluded, ignored);
assert.equal(excluded.feedback, undefined);
assert.equal(JSON.stringify([message(webhook), message(tier), message(session)])
    .includes('secret.example'), false);
"""
    result = subprocess.run(
        ["node", "-e", script, str(HELPER)],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_helper_is_included_only_on_settings() -> None:
    templates = ROOT / "src/web/templates"
    settings = (templates / "settings.html").read_text()
    assert settings.count('include "components/settings_save_feedback.js"') == 1
    assert 'settings_save_feedback.js' not in (templates / "base.html").read_text()
