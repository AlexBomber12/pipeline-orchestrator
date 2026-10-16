"""Browser checks for Settings saves using Chromium and the served HTMX script."""

from pathlib import Path

import pytest
from playwright.sync_api import Error, Page, Route, expect

EVIDENCE_DIR = Path(__file__).parent / "evidence"
NOTIFICATION_TIMEOUT = "guardrail_notification_timeout_seconds"
NOTIFICATION_TIMEOUT_URL = f"/settings/config/{NOTIFICATION_TIMEOUT}"


def _field(page: Page, name: str):
    return page.locator(f'[name="{name}"]')


def _status(page: Page, name: str):
    return page.locator(f'[name="{name}"] + [role="status"]')


def _message(page: Page, name: str):
    return _status(page, name).locator("span")


def _retry(page: Page, name: str):
    return _status(page, name).get_by_role("button", name="Retry save")


def _edit_and_blur(page: Page, name: str, value: str) -> None:
    field = _field(page, name)
    field.fill(value)
    field.blur()


@pytest.fixture
def settings_page(page: Page, dashboard_url: str, request: pytest.FixtureRequest) -> Page:
    errors: list[str] = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(f"{dashboard_url}/settings", wait_until="domcontentloaded")
    page.wait_for_function("Boolean(window.htmx && window.settingsSaveFeedbackInstalled)")
    yield page

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    page.screenshot(
        path=str(EVIDENCE_DIR / f"{request.node.name}.png"),
        full_page=True,
        mask=[_field(page, "guardrail_notification_webhook_url")],
    )
    assert not errors, f"Unexpected Settings page JavaScript errors: {errors!r}"


def test_real_notification_timeout_persists_and_restores(settings_page: Page, dashboard_url: str) -> None:
    page = settings_page
    original = _field(page, NOTIFICATION_TIMEOUT).input_value()
    changed = "12" if original != "12" else "13"
    try:
        with page.expect_response(lambda response: response.url.endswith(NOTIFICATION_TIMEOUT_URL)) as save:
            _edit_and_blur(page, NOTIFICATION_TIMEOUT, changed)
        assert save.value.status == 200
        expect(_message(page, NOTIFICATION_TIMEOUT)).to_have_text("Saved")
        page.reload()
        expect(_field(page, NOTIFICATION_TIMEOUT)).to_have_value(changed)
    finally:
        page.goto(f"{dashboard_url}/settings")
        if _field(page, NOTIFICATION_TIMEOUT).input_value() != original:
            with page.expect_response(lambda response: response.url.endswith(NOTIFICATION_TIMEOUT_URL)) as restored:
                _edit_and_blur(page, NOTIFICATION_TIMEOUT, original)
            assert restored.value.status == 200
            expect(_message(page, NOTIFICATION_TIMEOUT)).to_have_text("Saved")
        page.reload()
        expect(_field(page, NOTIFICATION_TIMEOUT)).to_have_value(original)


def test_real_rejected_url_keeps_edit_for_correction(settings_page: Page) -> None:
    page = settings_page
    name = "dashboard_base_url"
    rejected = "ftp://example.invalid/settings-test"
    with page.expect_response(lambda response: response.url.endswith(f"/settings/config/{name}")) as save:
        _edit_and_blur(page, name, rejected)
    assert save.value.status == 400
    expect(_message(page, name)).to_have_text("Not saved")
    expect(_field(page, name)).to_have_value(rejected)
    expect(_retry(page, name)).to_be_visible()


@pytest.mark.parametrize(
    "name",
    ["rate_limit_session_pause_percent", "rate_limit_weekly_pause_percent"],
)
def test_real_ignored_blank_provider_threshold_is_not_saved(settings_page: Page, name: str) -> None:
    page = settings_page
    with page.expect_response(
        lambda response: response.url.endswith("/settings/daemon") and response.request.method == "PUT"
    ) as save:
        _edit_and_blur(page, name, "")
    assert save.value.status == 200
    expect(_message(page, name)).to_have_text("Not saved")
    expect(_field(page, name)).to_have_value("")
    expect(_retry(page, name)).to_be_hidden()


def test_mocked_connection_failure_needs_explicit_retry(settings_page: Page) -> None:
    page = settings_page
    name = "spend_ceiling_warning_percent"
    attempts: list[str] = []

    def intercept(route: Route) -> None:
        attempts.append(route.request.method)
        if len(attempts) == 1:
            route.abort("failed")
        else:
            route.fulfill(status=200, body="Updated")

    page.route(f"**/settings/config/{name}", intercept)
    _edit_and_blur(page, name, "71")
    expect(_message(page, name)).to_have_text("Save not confirmed")
    page.wait_for_timeout(300)
    assert attempts == ["POST"], "a failed write must not retry automatically"
    _retry(page, name).click()
    expect(_message(page, name)).to_have_text("Saved")
    assert attempts == ["POST", "POST"]


def test_mocked_older_response_cannot_save_newer_edit(settings_page: Page) -> None:
    page = settings_page
    name = "spend_ceiling_warning_percent"
    held: list[Route] = []
    page.route(f"**/settings/config/{name}", lambda route: held.append(route))

    _edit_and_blur(page, name, "71")
    expect(_message(page, name)).to_have_text("Saving")
    assert len(held) == 1
    _edit_and_blur(page, name, "72")
    expect(_message(page, name)).to_have_text("Unsaved changes")
    held.pop(0).fulfill(status=200, body="Updated")

    # HTMX may queue the second change while the first XHR is active. Either
    # way, completion of the first response cannot confirm the newer value.
    page.wait_for_timeout(300)
    assert _field(page, name).input_value() == "72"
    assert _message(page, name).inner_text() != "Saved"
    if held:
        expect(_message(page, name)).to_have_text("Saving")
        held.pop(0).fulfill(status=200, body="Updated")
    else:
        expect(_message(page, name)).to_have_text("Unsaved changes")
        _retry(page, name).click()
        expect(_message(page, name)).to_have_text("Saving")
        assert len(held) == 1
        held.pop(0).fulfill(status=200, body="Updated")
    expect(_message(page, name)).to_have_text("Saved")


def test_mocked_timeout_is_unconfirmed_until_explicit_retry(settings_page: Page) -> None:
    page = settings_page
    name = "spend_ceiling_warning_percent"
    held: list[Route] = []
    attempts = 0

    def intercept(route: Route) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            held.append(route)
        else:
            route.fulfill(status=200, body="Updated")

    page.route(f"**/settings/config/{name}", intercept)
    _edit_and_blur(page, name, "73")
    expect(_message(page, name)).to_have_text("Saving")
    expect(_message(page, name)).to_have_text("Save not confirmed", timeout=16000)
    assert attempts == 1
    expect(_retry(page, name)).to_be_visible()
    _retry(page, name).click()
    expect(_message(page, name)).to_have_text("Saved")
    assert attempts == 2
    try:
        held[0].abort()
    except Error:
        # Chromium may already have canceled the timed-out XHR.
        pass


def test_mocked_select_change_uses_field_feedback(settings_page: Page) -> None:
    page = settings_page
    name = "guardrail_notification_min_tier"
    initial = _field(page, name).input_value()
    selected = "2" if initial == "1" else "1"
    page.route(
        f"**/settings/config/{name}",
        lambda route: route.fulfill(status=200, body="Updated"),
    )
    _field(page, name).select_option(selected)
    expect(_field(page, name)).to_have_value(selected)
    expect(_message(page, name)).to_have_text("Saved")


def test_mocked_webhook_test_is_separate_from_field_save(settings_page: Page) -> None:
    page = settings_page
    ordinary_saves: list[str] = []
    page.on(
        "request",
        lambda request: ordinary_saves.append(request.url)
        if "/settings/config/guardrail_notification_webhook_url" in request.url
        else None,
    )
    page.route(
        "**/settings/webhook/test",
        lambda route: route.fulfill(status=200, body="Synthetic test response"),
    )
    page.get_by_role("button", name="Test webhook").click()
    expect(page.locator("#webhook-test-status")).to_have_text("Synthetic test response")
    expect(_status(page, "guardrail_notification_webhook_url")).to_be_hidden()
    assert ordinary_saves == []
