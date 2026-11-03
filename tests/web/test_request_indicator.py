"""Deterministic contracts for the inline request controller, without Chromium."""

import subprocess
from pathlib import Path


def test_request_indicator_controller():
    template = Path('src/web/templates/base.html').read_text()
    controller = next(part.split('</script>')[0] for part in template.split('<script>')
                      if "const spinner =" in part)
    script = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
class Element {
    constructor() {
        this.hidden = true; this.listeners = {}; this.textContent = '';
        this.classes = new Set();
        this.classList = {toggle: (name, on) => on ? this.classes.add(name) : this.classes.delete(name)};
    }
    closest(selector) {return this.owner && selector === '[data-own-error]' ? this : null;}
    addEventListener(type, callback) {this.listeners[type] = callback;}
}
const ids = Object.fromEntries(['global-spinner', 'global-spinner-label', 'request-error',
    'request-error-message', 'request-error-dismiss', 'request-error-refresh'].map(id => [id, new Element()]));
const body = new Element(); let refreshes = 0;
vm.runInNewContext(process.argv[1], {Element, document: {body, getElementById: id => ids[id]},
    location: {reload: () => refreshes++}});
const fire = (type, detail) => body.listeners['htmx:' + type]({detail});
const request = verb => ({elt: new Element(), xhr: {}, requestConfig: {verb, triggeringEvent: {}}});
const read = request('GET'), write = request('POST');
fire('beforeRequest', read);
assert.equal(ids['global-spinner-label'].textContent, 'Loading…');
fire('beforeRequest', write);
fire('beforeRequest', write); // XHR identity is counted once.
assert.equal(ids['global-spinner-label'].textContent, 'Saving…');
fire('afterRequest', write);
assert(body.classes.has('htmx-request'));
assert.equal(ids['global-spinner-label'].textContent, 'Loading…');
fire('afterRequest', write); // duplicate completion cannot hide the remaining read
assert(body.classes.has('htmx-request'));
fire('afterRequest', read);
assert(!body.classes.has('htmx-request'));
read.xhr = {status: 503, responseText: 'read unavailable'};
fire('responseError', read);
assert(ids['request-error'].hidden);
write.xhr = {status: 503, responseText: '{"detail":"<b>Redis unavailable</b>"}'};
fire('responseError', write);
assert(!ids['request-error'].hidden);
assert.match(ids['request-error-message'].textContent, /HTTP 503: <b>Redis unavailable<\/b>/);
assert.match(ids['request-error-message'].textContent, /Outcome not confirmed; refresh state/);
ids['request-error-refresh'].listeners.click(); assert.equal(refreshes, 1);
ids['request-error-dismiss'].listeners.click(); assert(ids['request-error'].hidden);
write.elt.owner = true;
fire('responseError', write); fire('sendError', write);
assert(ids['request-error'].hidden);
write.elt.owner = false; write.xhr = {status: 0};
fire('sendError', write);
assert.match(ids['request-error-message'].textContent, /Transport error.*Outcome not confirmed/);
"""
    subprocess.run(['node', '-e', script, controller], check=True, capture_output=True, text=True)
