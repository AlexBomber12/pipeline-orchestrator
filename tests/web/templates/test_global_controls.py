"""Tests for the header global pause/resume/stop controls (PR-340).

PR-339 shipped daemon-wide endpoints (``/daemon/pause``, ``/daemon/resume``,
``/daemon/stop``, ``/daemon/drain-progress``) but no UI surface for them.
PR-340 adds three header buttons that consume those endpoints with
confirmation modals on the destructive paths (Pause All, Stop All) and an
immediate fire on the reversible path (Resume All).

The tests render ``components/global_modals.html`` directly to assert the
button row, the modal copy, the HTMX endpoint wiring, and the inline JS
contract the cypress-style acceptance criteria in the spec rely on.
"""

from __future__ import annotations

from src.web import app as web_app


def _render_component() -> str:
    template = web_app.templates.env.get_template(
        "components/global_modals.html"
    )
    return template.render()


def test_header_renders_three_buttons() -> None:
    # The dashboard header must carry exactly one Pause All, Resume All,
    # and Stop All button so the operator surface matches the PR-339
    # endpoint trio. Buttons are identified by ``data-action`` so the
    # markup is robust to copy changes during follow-up sprints.
    rendered = _render_component()

    assert 'data-action="pause-all"' in rendered
    assert 'data-action="resume-all"' in rendered
    assert 'data-action="stop-all"' in rendered
    assert "Pause All" in rendered
    assert "Resume All" in rendered
    assert "Stop All" in rendered


def test_pause_all_button_opens_modal() -> None:
    # Pause All must wire its click to the modal opener, not directly to
    # the destructive endpoint, so the confirmation step is mandatory.
    rendered = _render_component()

    pause_btn = rendered.split('data-action="pause-all"', 1)[1].split(
        "</button>", 1
    )[0]
    assert 'onclick="openPauseAllModal()"' in pause_btn
    # The opener must be defined globally so the inline handler resolves.
    assert "window.openPauseAllModal" in rendered
    # The dialog itself must exist with the matching id.
    assert 'id="pause-all-modal"' in rendered
    assert "<dialog" in rendered


def test_pause_all_modal_shows_affected_count_placeholder() -> None:
    # The modal renders a placeholder for the affected-count span; the
    # opener fills it from ``/daemon/drain-progress`` before showing the
    # dialog. The static template only needs to expose the slot and the
    # fetch URL — the dynamic count is asserted via the JS contract test.
    rendered = _render_component()

    assert "Affected repos:" in rendered
    assert 'id="pause-all-affected-count"' in rendered
    assert "data-affected-count" in rendered
    assert "/daemon/drain-progress" in rendered


def test_pause_all_opener_reads_repos_length_from_drain_progress() -> None:
    # The opener must populate the affected-count slot from the length of
    # ``data.repos`` returned by /daemon/drain-progress (the endpoint
    # PR-339 ships). Encodes the contract so a future refactor does not
    # silently drop the count or read the wrong field.
    rendered = _render_component()

    assert "fetch('/daemon/drain-progress')" in rendered
    assert "data.repos" in rendered
    # The opener writes the count into the placeholder span before
    # showing the modal so the operator never sees a stale value.
    assert "String(repos.length)" in rendered
    assert "showModal()" in rendered


def test_confirm_pause_all_posts_to_daemon_pause_endpoint() -> None:
    # The Confirm pause all button must fire a POST to /daemon/pause via
    # HTMX. ``hx-target=body``/``hx-swap=none`` plus the reload hook
    # mirrors the spec so the dashboard re-renders after the sweep.
    rendered = _render_component()

    confirm_segment = rendered.split("data-pause-all-confirm", 1)[1].split(
        "</button>", 1
    )[0]
    assert 'hx-post="/daemon/pause"' in confirm_segment
    assert 'hx-target="body"' in confirm_segment
    assert 'hx-swap="none"' in confirm_segment
    assert "window.location.reload()" in confirm_segment


def test_stop_all_modal_describes_destructive_nature() -> None:
    # The stop modal must communicate the blast radius in plain language
    # so an operator cannot mistake it for the graceful pause path. Spec
    # requires "destructive" and the SIGTERM/SIGKILL kill semantics to
    # be visible before the operator confirms.
    rendered = _render_component()

    stop_segment = rendered.split('id="stop-all-modal"', 1)[1].split(
        "</dialog>", 1
    )[0]
    assert "destructive" in stop_segment.lower()
    assert "KILLED" in stop_segment
    assert "SIGTERM" in stop_segment
    assert "SIGKILL" in stop_segment


def test_confirm_stop_all_posts_to_daemon_stop_endpoint() -> None:
    rendered = _render_component()

    confirm_segment = rendered.split("data-stop-all-confirm", 1)[1].split(
        "</button>", 1
    )[0]
    assert 'hx-post="/daemon/stop"' in confirm_segment
    assert 'hx-target="body"' in confirm_segment
    assert 'hx-swap="none"' in confirm_segment
    assert "window.location.reload()" in confirm_segment


def test_resume_all_no_modal_required() -> None:
    # Resume All is reversible (a paused repo simply returns to its
    # queued state), so the spec deliberately omits the confirmation
    # modal. The button must POST /daemon/resume immediately without
    # routing through a dialog opener.
    rendered = _render_component()

    resume_segment = rendered.split('data-action="resume-all"', 1)[1].split(
        "</button>", 1
    )[0]
    assert 'hx-post="/daemon/resume"' in resume_segment
    # No onclick=open... handler — the button must fire immediately.
    assert "openResumeAllModal" not in rendered
    assert "openResume" not in rendered
    # There is no resume-all dialog element.
    assert 'id="resume-all-modal"' not in rendered


def test_cancel_buttons_close_modals_without_firing_endpoint() -> None:
    # Both cancel buttons must close the dialog via ``.close()`` and must
    # NOT carry any ``hx-post`` attribute that would accidentally fire
    # the destructive endpoint as the dialog dismisses.
    rendered = _render_component()

    pause_cancel = rendered.split("data-pause-all-cancel", 1)[1].split(
        "</button>", 1
    )[0]
    assert (
        "document.getElementById('pause-all-modal').close()" in pause_cancel
    )
    assert "hx-post" not in pause_cancel

    stop_cancel = rendered.split("data-stop-all-cancel", 1)[1].split(
        "</button>", 1
    )[0]
    assert (
        "document.getElementById('stop-all-modal').close()" in stop_cancel
    )
    assert "hx-post" not in stop_cancel


def test_confirm_buttons_show_loading_state_during_in_flight_request() -> None:
    # The HTMX confirm buttons must flip ``disabled`` and surface a
    # loading label between ``htmx:beforeRequest`` and ``htmx:afterRequest``
    # so a slow /daemon/pause does not allow a second click that would
    # double-fire the sweep. Encodes the cypress-style acceptance criterion
    # from the spec ("button shows loading state and is disabled").
    rendered = _render_component()

    assert "data-pause-all-confirm" in rendered
    assert "data-stop-all-confirm" in rendered
    assert "htmx:beforeRequest" in rendered
    assert "htmx:afterRequest" in rendered
    assert "button.disabled = true" in rendered
    assert "button.disabled = false" in rendered


def test_buttons_appear_in_dashboard_response() -> None:
    # End-to-end smoke that base.html includes the component so the
    # buttons reach the rendered dashboard HTML, not just the isolated
    # component. Rendering ``base.html`` directly avoids spinning the
    # full FastAPI app while still proving the include wiring.
    rendered = web_app.templates.env.get_template("base.html").render(
        title="Dashboard"
    )

    assert 'data-action="pause-all"' in rendered
    assert 'data-action="resume-all"' in rendered
    assert 'data-action="stop-all"' in rendered
    assert 'id="pause-all-modal"' in rendered
    assert 'id="stop-all-modal"' in rendered
