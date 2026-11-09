"""Keep Python-generated utilities explicit, including every helper branch."""

import ast
import re
from pathlib import Path

from src.web.routes.dashboard import _REPO_BADGE_PALETTE, _exit_reason_classes

ROOT = Path(__file__).resolve().parents[2]


def test_content_and_theme():
    config = (ROOT / "tailwind.config.js").read_text()
    assert "./src/web/templates/**/*.html" in config
    assert "./src/web/**/*.js" in config
    base = (ROOT / "src/web/templates/base.html").read_text()
    inline = base.split("tailwind.config = ", 1)[1].split(";", 1)[0]
    assert re.sub(r"\s", "", inline)[1:-1] in re.sub(r"\s", "", config)
    assert (ROOT / "src/web/static/tailwind.src.css").read_text().splitlines() == [
        "@tailwind base;",
        "@tailwind components;",
        "@tailwind utilities;",
    ]


def test_safelist_covers_every_python_class():
    config = (ROOT / "tailwind.config.js").read_text()
    safelist = set(ast.literal_eval(config.split("safelist: ", 1)[1].split("],", 1)[0] + "]"))
    expected = set()
    # Literal helper returns/palettes and HTML built in Python, including new helpers.
    for path in (ROOT / "src/web").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                expected.update(re.findall(r"(?<![\w-])(?:bg|text|border)-[\w/-]+", node.value))
                for value in re.findall(r'class="([^"]+)"', node.value):
                    expected.update(value.split())
    badge = (ROOT / "src/web/templates/components/subsource_badge.html").read_text()
    expected.update(re.findall(r"(?:bg|text|border)-[\w/-]+", badge))
    expected.update(" ".join(_REPO_BADGE_PALETTE).split())
    for reason in ("closed_unmerged", "success_merged", "rate_limit", "error", "unknown"):
        expected.update(_exit_reason_classes(reason).split())
    assert expected <= safelist, sorted(expected - safelist)
