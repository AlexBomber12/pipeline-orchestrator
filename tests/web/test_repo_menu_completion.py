"""Repository menu completion refresh contracts."""

import subprocess
from pathlib import Path


def test_menu_completion_refreshes_controls_without_discarding_upload_feedback():
    template = Path("src/web/templates/index.html").read_text()
    handler = template.split("grid.addEventListener('htmx:afterRequest', ", 1)[1].split(
        "\n        });", 1
    )[0]
    script = r"""
const assert = require('node:assert/strict');
let refreshes = 0;
const document = {body: {}};
const htmx = {trigger: (target, name) => {
    assert.equal(target, document.body);
    assert.equal(name, 'repo:history_updated');
    refreshes++;
}};
const complete = eval('(' + process.argv[1] + ')');
const menu = {open: true};
const fire = (swap, successful, owner = menu) => complete({
    target: {closest: () => owner, getAttribute: () => swap},
    detail: {successful}
});
fire('innerHTML', true); // Upload feedback has already been swapped into the card.
assert.equal(menu.open, false);
assert.equal(refreshes, 0);
menu.open = true;
fire('none', false);
assert.equal(menu.open, true);
assert.equal(refreshes, 0);
fire('none', true);
assert.equal(menu.open, false);
assert.equal(refreshes, 1);
fire('none', true, null);
assert.equal(refreshes, 1);
"""
    subprocess.run(['node', '-e', script, handler + '}'], check=True, capture_output=True, text=True)
