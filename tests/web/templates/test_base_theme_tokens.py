"""Semantic palette and contrast contracts, independent of the browser CDN."""

import re
import subprocess
from pathlib import Path

import pytest

BASE = Path('src/web/templates/base.html').read_text()
PALETTE = {
    'dark': ['137 195 255', '68 214 214', '171 185 254', '224 164 238',
             '131 212 148', '253 161 154', '235 178 95', '156 163 175', '18 18 26', '26 6 6'],
    'light': ['4 101 175', '1 114 114', '78 89 177', '133 68 148',
              '8 119 54', '164 59 56', '134 89 1', '71 85 105', '255 255 255', '255 255 255'],
}
ROLES = 'accent coding watch fix ok fail warn muted on-primary on-fail'.split()


def contrast(first, second):
    def luminance(rgb):
        channels = [c / 255 for c in rgb]
        linear = [c / 12.92 if c <= .04045 else ((c + .055) / 1.055) ** 2.4 for c in channels]
        return sum(c * w for c, w in zip(linear, [.2126, .7152, .0722]))
    values = sorted([luminance(first), luminance(second)])
    return (values[1] + .05) / (values[0] + .05)


@pytest.mark.parametrize('theme', PALETTE)
def test_palette_and_contrast(theme):
    block = BASE.split(f'html[data-theme="{theme}"] {{')[1].split('}')[0]
    for cause in ['crash', 'escalate', 'timeout', 'infra', 'deadlock']:
        assert f'--cause-{cause}: var(--fail)' in block
    assert '--cause-operator: var(--text-2)' in block
    assert '--cause-legacy: var(--text-3)' in block
    colors = dict(re.findall(r'--c-([\w-]+): ([\d ]+);', block))
    assert colors == dict(zip(ROLES, PALETTE[theme]))
    rgb = {role: list(map(int, value.split())) for role, value in colors.items()}
    surface = [18, 18, 26] if theme == 'dark' else [255, 255, 255]
    for role in ROLES[:8]:
        assert contrast(rgb[role], surface) >= 4.5, role
        assert f"'{role}': 'rgb(var(--c-{role}) / <alpha-value>)'" in BASE
    for fill, foreground in [('accent', 'on-primary'), ('warn', 'on-primary'), ('fail', 'on-fail')]:
        assert contrast(rgb[fill], rgb[foreground]) >= 4.5


def test_aliases_and_no_semantic_overrides():
    for alias, role in [('merge', 'ok'), ('attn', 'warn'), ('hung', 'fail'), ('recover', 'muted')]:
        assert f"'{alias}': 'rgb(var(--c-{role}) / <alpha-value>)'" in BASE
    assert '--accent-rgb' not in BASE
    assert not re.search(r'\.(?:bg|text|border)-(?:accent|ok|warn|fail|hung)[^{}]*\{', BASE)
    for component in ['tasks_panel', 'cancellation_history']:
        assert 'amber-' not in Path(f'src/web/templates/components/{component}.html').read_text()
    for cause in ['crash', 'escalate', 'timeout', 'infra', 'deadlock']:
        assert f'--cause-{cause}: var(--fail)' in BASE
    assert '--cause-operator: var(--text-2)' in BASE
    assert '--cause-legacy: var(--text-3)' in BASE
    assert '--text-3: #8b929e' in BASE


def test_theme_preference_controller():
    controller = next(part.split('</script>')[0] for part in BASE.split('<script>')
                      if "const STORAGE_KEY = 'theme'" in part)
    script = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
for (const stored of [null, 'invalid', 'system', 'light', 'dark']) {
    for (const blocked of [false, true]) {
        const listeners = {}, controls = [{value: ''}, {value: ''}];
        const root = {dataset: {}, style: {}};
        const media = {matches: false, addEventListener: (_, fn) => media.change = fn};
        let saved = stored;
        const document = {
            documentElement: root, querySelectorAll: () => controls,
            addEventListener: (type, fn) => listeners[type] = fn,
        };
        const localStorage = {
            getItem: () => { if (blocked) throw Error('blocked'); return saved; },
            setItem: (key, value) => { if (blocked) throw Error('blocked'); saved = value; },
        };
        vm.runInNewContext(process.argv[1], {document, localStorage, window: {matchMedia: () => media}});
        listeners.DOMContentLoaded();
        const mode = !blocked && ['light', 'dark'].includes(stored) ? stored : 'system';
        assert.deepEqual(controls.map(c => c.value), [mode, mode]);
        assert.equal(root.dataset.theme, mode === 'light' ? 'light' : 'dark');
        media.matches = true; media.change();
        assert.equal(root.dataset.theme, mode === 'dark' ? 'dark' : 'light');
        for (const [index, choice] of [[0, 'dark'], [1, 'light'], [0, 'system']]) {
            const target = controls[index]; target.value = choice; target.matches = () => true;
            listeners.change({target});
            assert.deepEqual(controls.map(c => c.value), [choice, choice]);
            if (!blocked) assert.equal(saved, choice);
            for (const matches of [false, true]) {
                media.matches = matches; media.change();
                const expected = choice === 'system' ? (matches ? 'light' : 'dark') : choice;
                assert.equal(root.dataset.theme, expected);
                assert.equal(root.style.colorScheme, expected);
            }
        }
    }
}
"""
    subprocess.run(['node', '-e', script, controller], check=True, capture_output=True, text=True)
