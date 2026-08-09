"""Tests for ddpui.utils.email_templates.

Load-bearing goal: after refactoring the outer email shell into a single
``_render_email_shell`` helper, the existing report + mention emails MUST
still render byte-identically to the pre-refactor output. The golden fixtures
under ``fixtures/email_templates/`` were captured from the pre-refactor code
and pinned here — a diff means the shell extraction subtly changed something.
"""

from __future__ import annotations

import ast
import datetime
import re
from pathlib import Path

import pytest

from types import SimpleNamespace

from ddpui.utils import email_templates
from ddpui.utils.email_templates import (
    _render_trial_checklist,
    _render_trial_testimonial,
    _render_trial_text_link,
    render_alert_email,
    render_mention_email,
    render_share_report_email,
    render_verify_email,
    render_trial_welcome_email,
    render_trial_midpoint_email,
    render_trial_pre_end_email,
    render_trial_post_deletion_email,
)


def _fake_alert(name: str = "High Errors", id_: int = 7) -> SimpleNamespace:
    """A stand-in for a persisted Alert row — render_alert_email only needs .name + .id."""
    return SimpleNamespace(name=name, id=id_)


FIXTURES = Path(__file__).parent / "fixtures" / "email_templates"


def _read(fixture: str) -> str:
    return (FIXTURES / fixture).read_text()


# ── Golden: report share email ────────────────────────────────────────────


def test_report_email_matches_golden_with_public_url():
    plain, html = render_share_report_email(
        sender_name="Priya <b>Sharma</b>",
        report_title="Quarterly & Report",
        private_url="https://app.dalgo.org/reports/42",
        public_url="https://app.dalgo.org/public/tok123",
    )
    assert plain == _read("golden_report_plain.txt")
    assert html == _read("golden_report_html.html")


def test_report_email_matches_golden_without_public_url():
    plain, html = render_share_report_email(
        sender_name="Priya",
        report_title="Monthly",
        private_url="https://app.dalgo.org/reports/43",
    )
    assert plain == _read("golden_report_no_public_plain.txt")
    assert html == _read("golden_report_no_public_html.html")


# ── Golden: mention email ─────────────────────────────────────────────────


def test_mention_email_matches_golden():
    plain, html = render_mention_email(
        author_name="Priya <b>",
        author_email="priya@example.com",
        comment_excerpt="Check @vinit@example.com this <script>alert(1)</script>",
        snapshot_title="Q3 Report",
        report_url="https://app.dalgo.org/reports/7",
        thread=[
            {
                "author_name": "Vinit",
                "author_email": "vinit@example.com",
                "content": "Earlier @priya@example.com msg",
            }
        ],
        chart_name="Enrollments Chart",
    )
    assert plain == _read("golden_mention_plain.txt")
    assert html == _read("golden_mention_html.html")


def test_mention_email_matches_golden_minimal():
    plain, html = render_mention_email(
        author_name="Vinit",
        author_email="vinit@example.com",
        comment_excerpt="Hi",
        snapshot_title="Report",
        report_url="https://app.dalgo.org/r/1",
    )
    assert plain == _read("golden_mention_minimal_plain.txt")
    assert html == _read("golden_mention_minimal_html.html")


# ── render_alert_email ────────────────────────────────────────────────────


def test_render_alert_email_basic_shape():
    alert = _fake_alert(name="Enrollments low", id_=42)
    plain, html_ = render_alert_email(alert, "Current value is 12 (below 50).")

    # Plain-text contract
    assert plain.startswith("Alert fired: Enrollments low\n")
    assert "Current value is 12 (below 50)." in plain
    assert "View alert:" in plain

    # HTML contract — shell + body
    assert "Dalgo" in html_  # wordmark
    assert "#00897B" in html_  # brand teal from shell
    assert "Alert fired: Enrollments low" in html_
    assert "Current value is 12 (below 50)." in html_
    assert "View alert" in html_


def test_render_alert_email_escapes_html_in_user_body(monkeypatch):
    monkeypatch.setenv("FRONTEND_URL", "https://app.dalgo.org")
    alert = _fake_alert()
    _, html_ = render_alert_email(alert, "<script>alert(1)</script>")

    assert "<script>alert(1)</script>" not in html_
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html_


def test_render_alert_email_converts_newlines_to_br():
    alert = _fake_alert()
    _, html_ = render_alert_email(alert, "line1\nline2\nline3")

    # HTML-escape happens before <br> conversion — < in user text would be &lt;
    assert "line1<br>\nline2<br>\nline3" in html_


def test_render_alert_email_cta_url_points_to_alerts_page(monkeypatch):
    monkeypatch.setenv("FRONTEND_URL", "https://app.dalgo.org")
    alert = _fake_alert(id_=99)
    plain, html_ = render_alert_email(alert, "body")

    expected = "https://app.dalgo.org/alerts"
    assert expected in plain
    assert expected in html_
    # No per-alert deep-link yet — the alerts listing has no ?highlight handler.
    assert "highlight=" not in html_


def test_render_alert_email_escapes_alert_name_in_headline():
    alert = _fake_alert(name="<b>Injected</b>")
    _, html_ = render_alert_email(alert, "body")
    # The literal <b> must not survive into the HTML headline
    assert "<b>Injected</b>" not in html_
    assert "&lt;b&gt;Injected&lt;/b&gt;" in html_


# ── Trial lifecycle emails ────────────────────────────────────────────────


def test_verify_email_has_headline_link_and_expiry_note():
    plain, html_body = render_verify_email("https://app.dalgo.org/verify/tok123")
    assert "Welcome to Dalgo" in plain and "Welcome to Dalgo" in html_body
    assert "https://app.dalgo.org/verify/tok123" in html_body
    assert "VERIFY EMAIL" in html_body
    assert "expires in 24 hours" in plain
    # No trial badge on this one — account doesn't exist yet.
    assert "Trial ·" not in html_body


def test_welcome_email_lists_three_actions_and_badge():
    plain, html_body = render_trial_welcome_email("https://app.dalgo.org/impact", trial_days=14)
    assert "Your workspace is ready" in html_body
    assert "Trial · 14 days</span>" in html_body
    for title in (
        "Explore the platform",
        "Build your first insight",
        "Build an automated data pipeline",
    ):
        assert title in html_body and title in plain
    assert "OPEN MY WORKSPACE" in html_body
    assert "https://app.dalgo.org/impact" in html_body


def test_midpoint_email_computes_days_left_and_shows_both_ctas():
    plain, html_body = render_trial_midpoint_email(
        day_number=7,
        total_days=14,
        upgrade_url="https://app.dalgo.org/upgrade",
        schedule_call_url="https://cal.com/dalgo",
    )
    assert "Trial · 7 days left</span>" in html_body
    assert "Day 7 of 14" in html_body
    assert "You're halfway through your trial period" in html_body
    assert "UPGRADE" in html_body and "SCHEDULE A CALL" in html_body
    assert "https://app.dalgo.org/upgrade" in html_body
    assert "https://cal.com/dalgo" in html_body
    assert "7 days left" in plain


def test_pre_end_email_shows_danger_gradient_and_end_date():
    _, html_body = render_trial_pre_end_email(
        day_number=12,
        total_days=14,
        end_date="15 Aug 2026",
        upgrade_url="https://app.dalgo.org/upgrade",
        schedule_call_url="https://cal.com/dalgo",
    )
    assert "2 days left in your trial" in html_body
    assert "Trial · 2 days left</span>" in html_body
    assert "15 Aug 2026" in html_body
    assert "permanently deleted" in html_body
    assert "linear-gradient" in html_body  # the danger-mode progress fill


def test_post_deletion_email_has_testimonial_and_no_badge():
    plain, html_body = render_trial_post_deletion_email("https://cal.com/dalgo")
    assert "Thanks for building with us" in html_body
    assert "permanently" not in html_body  # workspace is already gone, different copy
    assert "WHY TEAMS STAY" in html_body
    assert "BHUMI" in html_body
    assert "SCHEDULE A CALL" in html_body
    assert "Trial ·" not in html_body
    assert "BHUMI" in plain


def test_trial_emails_escape_untrusted_input():
    """end_date/urls are the only caller-supplied strings in these 5 — confirm
    they're HTML-escaped like every other render_* in this module."""
    _, html_body = render_trial_pre_end_email(
        day_number=12,
        total_days=14,
        end_date="<script>alert(1)</script>",
        upgrade_url="https://x/y",
        schedule_call_url="https://x/z",
    )
    assert "<script>" not in html_body
    assert "&lt;script&gt;" in html_body


# ── Shell single-source-of-truth guard ───────────────────────────────────


def test_shell_is_single_source_of_truth():
    """Assert each email chrome variant is defined in exactly one place.

    Two shells exist by design (see the module note above ``_DALGO_NAVY``):
    ``_render_email_shell`` (teal banner, in-product notifications) and
    ``_render_trial_email_shell`` (white header + navy wordmark, trial
    lifecycle emails). Every render_* function must delegate to one of them —
    neither shell's chrome may be inlined anywhere else.
    """
    source = Path(email_templates.__file__).read_text()
    module = ast.parse(source)

    # Locate every function definition and its source range.
    fns = {}
    for node in module.body:
        if isinstance(node, ast.FunctionDef):
            fns[node.name] = source.splitlines()[node.lineno - 1 : node.end_lineno]

    assert "_render_email_shell" in fns, "shell helper must exist"
    assert "_render_trial_email_shell" in fns, "trial shell helper must exist"

    shell_body = "\n".join(fns["_render_email_shell"])
    trial_header_body = "\n".join(fns["_render_trial_header"])

    # Every render_* function must delegate to one of the two shells — no inline shell.
    for name, body_lines in fns.items():
        if not name.startswith("render_"):
            continue
        body = "\n".join(body_lines)
        assert "_render_email_shell" in body or "_render_trial_email_shell" in body, (
            f"{name} must call _render_email_shell or _render_trial_email_shell"
            " — do not inline the shell"
        )

    # The teal-banner shell's wordmark markup lives ONLY inside the shell.
    # Anywhere else in the module is fine to reference the color for accents (e.g.
    # a link color) — the ban is specifically on the header/wordmark block.
    wordmark_fragment = '<h1 style="color:#ffffff; margin:0; font-size:18px; font-weight:700; letter-spacing:0.5px;">Dalgo</h1>'
    assert wordmark_fragment in shell_body, "shell must render the Dalgo wordmark"

    # Wordmark appears once in the whole module — only inside _render_email_shell.
    assert (
        source.count(wordmark_fragment) == 1
    ), "Dalgo wordmark HTML fragment must appear in exactly one place — inside _render_email_shell"

    # Same single-source guard for the trial shell's navy wordmark span. This checks
    # the literal f-string SOURCE (the `{_DALGO_NAVY}` placeholder, unsubstituted),
    # not a rendered value — trial_header_body is raw source text, not output.
    trial_wordmark_fragment = (
        '<span style="color:{_DALGO_NAVY}; font-size:22px; font-weight:800;">Dalgo</span>'
    )
    assert (
        trial_wordmark_fragment in trial_header_body
    ), "_render_trial_header must render the navy Dalgo wordmark"
    assert (
        source.count(trial_wordmark_fragment) == 1
    ), "Navy Dalgo wordmark fragment must appear in exactly one place — inside _render_trial_header"


def test_report_and_alert_share_same_shell_chrome():
    """Two different renderers, same shell chrome — asserts the shared shell
    surrounds both outputs identically.

    We slice out the body cell (which differs per email type) and compare the
    surrounding shell fragments.
    """
    _, report_html = render_share_report_email(
        sender_name="X", report_title="T", private_url="https://x/y"
    )
    _, alert_html = render_alert_email(_fake_alert(), "body")

    # Prefix through the opening body <td> is shell-owned and must match.
    prefix = report_html.split('<td style="padding:32px;">', 1)[0]
    alert_prefix = alert_html.split('<td style="padding:32px;">', 1)[0]
    assert prefix == alert_prefix

    # Suffix from the footer border-top through closing </html> is shell-owned.
    suffix_marker = '<td style="padding:16px 32px; border-top:1px solid #e5e7eb;">'
    report_suffix = report_html.split(suffix_marker, 1)[1].split("</p>", 1)[1]
    alert_suffix = alert_html.split(suffix_marker, 1)[1].split("</p>", 1)[1]
    assert report_suffix == alert_suffix


# ── Internal subscription-request email (plain text) ──────────────────────


def _fake_request_actors(
    work_domain: str | None = "monitoring_evaluation",
    full_name: str = "Himanshu Dube",
    role_name: str | None = "Admin",
    base_plan: str | None = "Free Trial",
) -> tuple:
    """Stand-ins for a persisted Org + OrgUser + OrgPlans — only these attributes are read."""
    org = SimpleNamespace(
        name="Noora Health",
        slug="noora-health",
        created_at=datetime.datetime(2026, 8, 1, 9, 12, tzinfo=datetime.timezone.utc),
    )
    first, _, last = full_name.partition(" ")
    orguser = SimpleNamespace(
        user=SimpleNamespace(
            email="himanshu@projecttech4dev.org",
            get_full_name=lambda: full_name,
            first_name=first,
            last_name=last,
        ),
        work_domain=work_domain,
        new_role=SimpleNamespace(name=role_name) if role_name else None,
    )
    org_plan = SimpleNamespace(base_plan=base_plan)
    return org, orguser, org_plan


REQUESTED_AT = datetime.datetime(2026, 8, 8, 11, 4, tzinfo=datetime.timezone.utc)


def test_subscription_request_email_renders_both_blocks():
    org, orguser, org_plan = _fake_request_actors()
    subject, body = email_templates.build_subscription_request_email(
        org, orguser, org_plan, REQUESTED_AT
    )

    assert subject == "Subscription request: Noora Health"
    assert body == (
        "Org\n"
        "  Name:         Noora Health\n"
        "  Slug:         noora-health\n"
        "  Type:         Free Trial\n"
        "  Created:      2026-08-01 09:12 UTC\n"
        "\n"
        "Requested by\n"
        "  Name:         Himanshu Dube\n"
        "  Email:        himanshu@projecttech4dev.org\n"
        "  Job title:    Monitoring & Evaluation\n"
        "  Dalgo role:   Admin\n"
        "  Requested at: 2026-08-08 11:04 UTC\n"
    )


def test_subscription_request_email_falls_back_for_missing_values():
    """Trial signup collects no name and work_domain is nullable — neither may break the render."""
    org, orguser, org_plan = _fake_request_actors(
        work_domain=None, full_name="", role_name=None, base_plan=None
    )
    org.created_at = None

    _, body = email_templates.build_subscription_request_email(org, orguser, org_plan, REQUESTED_AT)

    assert "  Type:         —\n" in body
    assert "  Created:      —\n" in body
    assert "  Name:         —\n" in body
    assert "  Job title:    —\n" in body
    assert "  Dalgo role:   —\n" in body


def test_subscription_request_email_passes_through_unknown_work_domain():
    """A slug added to the form but not yet to WORK_DOMAIN_LABELS must still show up."""
    org, orguser, org_plan = _fake_request_actors(work_domain="brand_new_option")
    _, body = email_templates.build_subscription_request_email(org, orguser, org_plan, REQUESTED_AT)
    assert "  Job title:    brand_new_option\n" in body


def test_subscription_request_email_converts_datetimes_to_utc():
    org, orguser, org_plan = _fake_request_actors()
    ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    org.created_at = datetime.datetime(2026, 8, 1, 14, 42, tzinfo=ist)

    _, body = email_templates.build_subscription_request_email(org, orguser, org_plan, REQUESTED_AT)

    assert "  Created:      2026-08-01 09:12 UTC\n" in body


def test_work_domain_labels_cover_the_signup_form_options():
    """Guards the sync with TRIAL_ROLE_OPTIONS in the frontend's constants/trial.ts."""
    assert set(email_templates.WORK_DOMAIN_LABELS) == {
        "none",
        "monitoring_evaluation",
        "program_manager",
        "data_tech",
        "leadership",
        "consultant",
        "field_worker",
    }


# ── Trial email helper components ────────────────────────────────────────


def test_checklist_marks_done_and_pending_rows_differently():
    """a completed row shows the tick glyph, a pending row does not"""
    html_out = _render_trial_checklist(
        [
            (True, "Build your first insight", "Build out your first dashboard and share it"),
            (False, "Setup an automated data pipeline", "Setup your data to be updated"),
        ]
    )
    assert "Build your first insight" in html_out
    assert "Setup an automated data pipeline" in html_out
    # the green tick is drawn once — only for the completed row
    assert html_out.count("&#10003;") == 1


def test_checklist_escapes_titles_and_subtitles():
    """caller-supplied copy is escaped, never injected raw"""
    html_out = _render_trial_checklist([(False, "<script>x</script>", "a & b")])
    assert "<script>" not in html_out
    assert "&lt;script&gt;" in html_out
    assert "a &amp; b" in html_out


def test_testimonial_contains_quote_and_attribution():
    """the SEE WHAT'S POSSIBLE block is fixed copy, identical in all three emails"""
    html_out = _render_trial_testimonial()
    assert "SEE WHAT'S POSSIBLE" in html_out
    assert "Anindita" in html_out
    assert "SNEHA" in html_out


def test_text_link_renders_label_and_href_with_arrow():
    """the footer link carries its label, the url, and the trailing arrow glyph"""
    html_out = _render_trial_text_link("Schedule a call with us", "https://cal.example/x")
    assert 'href="https://cal.example/x"' in html_out
    assert "Schedule a call with us" in html_out
    assert "&#8599;" in html_out


def test_text_link_escapes_url():
    """a url containing a quote cannot break out of the href attribute"""
    html_out = _render_trial_text_link("Call", 'https://x/"onmouseover="alert(1)')
    assert 'onmouseover="alert(1)' not in html_out
    assert "&quot;" in html_out
