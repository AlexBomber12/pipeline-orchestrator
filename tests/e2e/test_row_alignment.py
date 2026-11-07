"""Chromium layout contract using production templates and synthetic task data."""

import pytest
from playwright.sync_api import expect
from src.web.app import templates


@pytest.mark.parametrize("width", [360, 640, 1280])
@pytest.mark.parametrize("retry_state", [None, "queued", "processing", "deferred", "applied", "capped"])
def test_row_alignment(page, dashboard_url, width, retry_state):
    long_value = "long-unbroken-value-" * 12
    task = dict(
        pr_id="PR-499", title=long_value, branch=long_value, retry_count=0,
        retry_binding="synthetic-binding", retry_command=None,
    )
    if retry_state == "capped":
        task["retry_count"] = 3
    elif retry_state:
        task["retry_command"] = dict(
            status=retry_state, command_id=long_value, outcome_reason=long_value,
            execution_state=long_value,
        )
    html = templates.env.from_string(
        '{% extends "base.html" %}{% block content %}'
        '{% include "components/tasks_panel.html" %}{% endblock %}'
    ).render(
        repo_name="example__alpha", tasks_total=1, retry_cap=3,
        tasks_by_status=dict(doing=[], todo=[], error=[task], done=[]),
    )
    diagnostic = templates.env.get_template("components/diagnostic_panel.html").module.diagnostic_panel(
        dict(task_id="PR-499", repo_slug="example__alpha", frontmatter_status=long_value,
             current_run_started_at=long_value, retry_count=1, attempt_count=2,
             current_pr=dict(number=499, state=long_value, url="https://example.invalid/499")),
        3, reset_button_enabled=True,
    )
    page.route(f"{dashboard_url}/row-alignment", lambda route: route.fulfill(body=html, content_type="text/html"))
    page.route("**/PR-499/diagnostic", lambda route: route.fulfill(body=diagnostic, content_type="text/html"))
    page.route("**/tasks/PR-499", lambda route: route.fulfill(body="<pre>Task content</pre>"))
    page.set_viewport_size(dict(width=width, height=900))
    page.goto(f"{dashboard_url}/row-alignment", wait_until="load")
    page.wait_for_function(
        "window.htmx && getComputedStyle(document.querySelector('[data-task-row]')).display === 'grid'"
    )
    actions = page.locator("[data-task-actions]")
    retry = actions.get_by_role("button", name="Retry", exact=False)
    if retry_state:
        expect(retry).to_be_disabled()
    else:
        expect(retry).to_be_enabled()
        assert retry.get_attribute("hx-vals") == '{"retry_binding":"synthetic-binding"}'
    actions.get_by_role("button", name="Diagnostic", exact=True).click()
    expect(page.locator("[data-diagnostic-panel]")).to_be_visible()
    page.locator("[data-task-row] > button").click()
    expect(page.locator("[id='task-content-error-PR-499']")).to_have_text("Task content")
    page.wait_for_function("getComputedStyle(document.querySelector('dl')).display === 'grid'")
    violations = page.locator("#repo-tasks-panel-example__alpha").evaluate("""panel => {
        const issues = [];
        for (const element of panel.querySelectorAll('*')) {
            if (!element.getClientRects().length) continue;
            const box = element.getBoundingClientRect(), parent = element.parentElement.getBoundingClientRect();
            if (box.left < parent.left - 1 || box.right > parent.right + 1 ||
                element.scrollWidth > element.clientWidth + 1 && getComputedStyle(element).display !== 'inline')
                issues.push(element.outerHTML.slice(0, 180));
        }
        const row = panel.querySelector('[data-task-row]');
        const info = row.firstElementChild.getBoundingClientRect();
        const actions = row.lastElementChild.getBoundingClientRect();
        if (info.right > actions.left || actions.right < row.getBoundingClientRect().right - 1)
            issues.push('actions must stay to the right of task details');
        const buttons = [...row.lastElementChild.children].map(e => e.getBoundingClientRect());
        if (Math.abs(buttons[0].y + buttons[0].height / 2 - buttons[1].y - buttons[1].height / 2) > 1)
            issues.push('actions must share one line');
        return issues;
    }""")
    assert not violations, violations
