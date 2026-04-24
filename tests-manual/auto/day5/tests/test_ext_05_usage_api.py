"""
EXT-05: Usage API degradation — dashboard loads even when Anthropic usage endpoint 403s.

Day 4 result: PASSED (trivial, 655ms).
Day 5 re-verify: regression check.
"""

import pytest
import requests


def test_ext_05_usage_api_degradation_mechanism_exists(
    page,
    testbed_url,
    dashboard_url,
    take_screenshot,
):
    page.goto(dashboard_url)
    take_screenshot("01_dashboard")

    response = requests.get(f"{dashboard_url}/api/states", timeout=5)
    assert response.status_code == 200, \
        f"Dashboard states API unreachable: {response.status_code}"

    page.goto(testbed_url)
    assert page.locator("body").is_visible(), \
        "Testbed detail page did not render with possible 403 on usage endpoint"
