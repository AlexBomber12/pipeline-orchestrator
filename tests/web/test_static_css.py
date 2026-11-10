"""Serving decisions use PR-510 build evidence, including live source changes."""

import hashlib
import json
import logging
import shutil

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from scripts.build_tailwind import ROOT, build
from src.web import app as web_app
from src.web.routes.dashboard import partial_redis_banner


@pytest.fixture
def css_site(tmp_path, monkeypatch):
    root = tmp_path / "source"
    for name in ("tailwind.config.js", "src/web/static/tailwind.src.css", "scripts/build_tailwind.py"):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / name, target)
    shutil.copytree(ROOT / "src/web/templates", root / "src/web/templates")
    (root / "src/web/templates/css_child.html").write_text(
        '{% extends "base.html" %}{% block title %}Inherited CSS page{% endblock %}'
    )
    compiler = tmp_path / "tailwindcss"
    compiler.write_text('#!/bin/sh\nwhile [ "$1" != "-o" ]; do shift; done\nprintf ".text-ok{}" > "$2"\n')
    compiler.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:/usr/bin:/bin")
    output = tmp_path / "static"
    monkeypatch.setattr(web_app, "STATIC_DIR", output)
    monkeypatch.setattr(web_app, "_CSS_ROOT", root)
    monkeypatch.setattr(web_app, "_css_fallback_reported", False)
    monkeypatch.setattr(web_app.templates.env.loader, "searchpath", [str(root / "src/web/templates")])
    # Exercise the mounted production StaticFiles handler without Redis/lifespan.
    static = next(route.app for route in web_app.app.routes if route.path == "/static")
    monkeypatch.setattr(static, "directory", output)
    monkeypatch.setattr(static, "all_directories", [output])
    monkeypatch.setattr(static, "config_checked", False)
    site = FastAPI()
    site.mount("/static", static, name="static")

    @site.get("/")
    def page(request: Request):
        return web_app.templates.TemplateResponse(request, "base.html", {})

    @site.get("/inherited")
    async def inherited_page(request: Request):
        return web_app.templates.TemplateResponse(request, "css_child.html", {})

    site.add_api_route("/partials/redis-banner", partial_redis_banner)

    with TestClient(site) as client:
        yield root, output, compiler, client


def assert_fallback(client):
    body = client.get("/").text
    assert "https://cdn.tailwindcss.com" in body
    assert "tailwind.config =" in body
    assert '/static/app.css?v=' not in body


def test_missing_then_first_compile(css_site, caplog):
    root, output, compiler, client = css_site
    caplog.set_level(logging.WARNING, logger=web_app.__name__)
    assert_fallback(client)
    assert_fallback(client)
    assert len(caplog.records) == 1
    assert "CDN fallback" in caplog.text
    assert build(root, output, compiler)
    css = (output / "app.css").read_bytes()
    digest = hashlib.sha256(css).hexdigest()
    body = client.get("/").text
    assert "cdn.tailwindcss.com" not in body
    assert "tailwind.config =" not in body
    assert f'/static/app.css?v={digest}' in body
    response = client.get(f"/static/app.css?v={digest}")
    assert response.status_code == 200
    assert response.content == css
    assert response.headers["content-type"].startswith("text/css")
    assert client.get("/static/missing.css").status_code == 404
    # Same running app notices a bind-mounted source edit and reports transition once.
    template = root / "src/web/templates/base.html"
    template.write_text(template.read_text() + "<!-- mounted source edit -->")
    assert_fallback(client)
    assert_fallback(client)
    assert len(caplog.records) == 2


@pytest.mark.parametrize("mode", ["stale_failed", "partial", "empty", "digest", "manifest", "compiler"])
def test_invalid_evidence_falls_back(css_site, mode):
    root, output, compiler, client = css_site
    assert build(root, output, compiler)
    original = (output / "app.css").read_bytes()
    if mode in {"stale_failed", "partial"}:
        (root / "src/web/templates/new.html").write_text('<p class="text-ok">new</p>')
        compiler.write_text('#!/bin/sh\nwhile [ "$1" != "-o" ]; do shift; done\nprintf partial > "$2"\nexit 1\n')
        assert not build(root, output, compiler)
        assert (output / "app.css").read_bytes() == original
        if mode == "partial":
            (output / "app.css").write_text("partial")
    elif mode == "empty":
        (output / "app.css").write_bytes(b"")
    elif mode == "digest":
        (output / "app.css").write_text("corrupt")
    elif mode == "manifest":
        (output / "app.css.manifest.json").write_text(json.dumps({}))
    else:
        compiler.unlink()
    assert_fallback(client)


def test_asset_disappears_during_verification(css_site, monkeypatch):
    root, output, compiler, client = css_site
    assert build(root, output, compiler)

    def disappears(**kwargs):
        (output / "app.css").unlink()
        return True

    monkeypatch.setattr(web_app, "_css_is_current", disappears)
    assert_fallback(client)


def test_asset_changes_during_verification(css_site, monkeypatch):
    root, output, compiler, client = css_site
    assert build(root, output, compiler)

    def changes(**kwargs):
        (output / "app.css").write_text("replacement")
        return True

    monkeypatch.setattr(web_app, "_css_is_current", changes)
    assert_fallback(client)


def test_polled_fragment_skips_css_filesystem_work(css_site, monkeypatch, caplog):
    root, output, compiler, client = css_site
    assert build(root, output, compiler)

    def unexpected_css_work(*args, **kwargs):
        pytest.fail("Fragment responses must not read or verify compiled CSS")

    monkeypatch.setattr(web_app, "_css_digest", unexpected_css_work)
    monkeypatch.setattr(web_app, "_css_is_current", unexpected_css_work)
    for _ in range(3):
        response = client.get("/partials/redis-banner")
        assert response.status_code == 200
        assert "Redis not configured" in response.text
        assert "/static/app.css" not in response.text
    assert not caplog.records


def test_inherited_page_verifies_once_and_notices_source_edit(css_site, monkeypatch):
    root, output, compiler, client = css_site
    assert build(root, output, compiler)
    verify = web_app._css_is_current
    calls = []

    def counted_verification(**kwargs):
        calls.append(kwargs)
        return verify(**kwargs)

    monkeypatch.setattr(web_app, "_css_is_current", counted_verification)
    response = client.get("/inherited")
    assert response.status_code == 200
    assert "Inherited CSS page" in response.text
    digest = hashlib.sha256((output / "app.css").read_bytes()).hexdigest()
    assert f'/static/app.css?v={digest}' in response.text
    assert "cdn.tailwindcss.com" not in response.text
    assert len(calls) == 1
    template = root / "src/web/templates/css_child.html"
    template.write_text(template.read_text() + "<!-- live edit -->")
    response = client.get("/inherited")
    assert response.status_code == 200
    assert "cdn.tailwindcss.com" in response.text
    assert '/static/app.css?v=' not in response.text
    assert len(calls) == 2
