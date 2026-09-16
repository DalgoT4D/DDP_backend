"""Tests for the trial-lifecycle + biz-dev email templates.

Notification-family template tests (report share golden, mention golden, alert
email, generic notification, cross-shell chrome) live in ``test_templates.py``
in the same directory.
"""

from __future__ import annotations

import ast
import datetime
from pathlib import Path

from types import SimpleNamespace
from typing import get_args

from ddpui.schemas.trial_schema import WorkDomain

from ddpui.core.notifications.templates import (
    build_new_org_signup_email,
    build_subscription_request_email,
)
from ddpui.core.notifications.templates.biz_dev import WORK_DOMAIN_LABELS
from ddpui.core.notifications.templates.trial import (
    render_trial_completion_email,
    render_trial_day3_in_progress_email,
    render_trial_day3_not_started_email,
    render_trial_midpoint_email,
    render_trial_post_deletion_email,
    render_trial_pre_end_email,
    render_trial_welcome_email,
    render_verify_email,
)
from ddpui.core.notifications.templates.trial_shell import (
    _render_trial_checklist,
    _render_trial_testimonial,
    _render_trial_text_link,
)


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


def test_midpoint_email_computes_days_left_and_shows_the_cta():
    plain, html_body = render_trial_midpoint_email(
        day_number=7,
        total_days=14,
        schedule_call_url="https://cal.com/dalgo",
    )
    assert "Trial · 7 days left</span>" in html_body
    assert "Day 7 of 14" in html_body
    assert "You're halfway through your trial period" in html_body
    assert "SCHEDULE A CALL" in html_body
    assert "UPGRADE" not in html_body
    assert "https://cal.com/dalgo" in html_body
    assert "7 days left" in plain


def test_pre_end_email_shows_danger_gradient_and_end_date():
    _, html_body = render_trial_pre_end_email(
        day_number=12,
        total_days=14,
        end_date="15 Aug 2026",
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


def test_cta_url_with_query_string_survives_round_trip():
    """A URL with a `&`-joined query string must appear in the CTA `href` with exactly one
    level of HTML-escaping.

    Before the fix, every caller pre-escaped the URL with ``html.escape`` and then handed
    the already-escaped string to ``_render_trial_cta_button``, which escaped it again —
    turning ``&src=trial`` into ``&amp;amp;src=trial``. A browser resolves ``&amp;`` back to
    a literal ``&``, so the second query parameter would actually arrive named ``amp;src``.
    """
    url = "https://app.dalgo.org/settings/billing?utm=email&src=trial"
    _, html_body = render_trial_midpoint_email(
        day_number=7,
        total_days=14,
        schedule_call_url=url,
    )
    assert "&amp;amp;" not in html_body
    assert 'href="https://app.dalgo.org/settings/billing?utm=email&amp;src=trial"' in html_body


def test_all_cta_call_sites_single_escape_a_query_string_url():
    """Every trial-lifecycle renderer with a CTA button must single-escape a query-string
    URL, not just the midpoint email exercised above."""
    url = "https://app.dalgo.org/x?a=1&b=2"
    expected_href = 'href="https://app.dalgo.org/x?a=1&amp;b=2"'

    renders = [
        render_verify_email(url),
        render_trial_welcome_email(url),
        render_trial_pre_end_email(
            day_number=12,
            total_days=14,
            end_date="15 Aug 2026",
            schedule_call_url=url,
        ),
        render_trial_post_deletion_email(url),
        render_trial_day3_not_started_email(url, "https://cal.example"),
        render_trial_day3_in_progress_email("insights", url, "https://cal.example"),
        render_trial_completion_email(url, "https://cal.example"),
    ]
    for _, html_body in renders:
        assert "&amp;amp;" not in html_body
        assert expected_href in html_body


def test_trial_emails_escape_untrusted_input():
    """end_date/urls are the only caller-supplied strings in these 5 — confirm
    they're HTML-escaped like every other render_* in this module."""
    _, html_body = render_trial_pre_end_email(
        day_number=12,
        total_days=14,
        end_date="<script>alert(1)</script>",
        schedule_call_url="https://x/z",
    )
    assert "<script>" not in html_body
    assert "&lt;script&gt;" in html_body


# ── Notification email (in-app notifications, email channel) ─────────────


# ── Trial shell single-source-of-truth guard ─────────────────────────────


def test_trial_shell_is_single_source_of_truth():
    """The trial-lifecycle shell (white header + navy Dalgo wordmark) lives
    in ``trial_shell._render_trial_header`` and nowhere else. Every
    ``render_trial_*`` in ``templates/trial.py`` must delegate through
    ``_render_trial_email_shell``.

    (The notification-family shell has its own guard in
    ``test_templates.py`` in the same directory.)
    """
    from ddpui.core.notifications.templates import trial as trial_module
    from ddpui.core.notifications.templates import trial_shell as trial_shell_module

    trial_shell_source = Path(trial_shell_module.__file__).read_text()
    trial_source = Path(trial_module.__file__).read_text()

    # Every render_* function in trial.py must delegate to _render_trial_email_shell.
    trial_ast = ast.parse(trial_source)
    for node in trial_ast.body:
        if isinstance(node, ast.FunctionDef) and node.name.startswith("render_"):
            body = "\n".join(trial_source.splitlines()[node.lineno - 1 : node.end_lineno])
            assert (
                "_render_trial_email_shell" in body
            ), f"{node.name} must call _render_trial_email_shell — do not inline the trial shell"

    # Navy wordmark fragment (source-level check — the `{_DALGO_NAVY}` placeholder
    # is inspected before f-string substitution).
    trial_wordmark_fragment = (
        '<span style="color:{_DALGO_NAVY}; font-size:22px; font-weight:800;">Dalgo</span>'
    )
    assert (
        trial_wordmark_fragment in trial_shell_source
    ), "trial_shell.py must render the navy Dalgo wordmark"
    # Wordmark appears once across BOTH files.
    assert (trial_shell_source + trial_source).count(
        trial_wordmark_fragment
    ) == 1, "Navy Dalgo wordmark fragment must appear in exactly one place — inside trial_shell.py"


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
    subject, body = build_subscription_request_email(org, orguser, org_plan, REQUESTED_AT)

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
        "  Function:     Monitoring and Evaluation\n"
        "  Dalgo role:   Admin\n"
        "  Requested at: 2026-08-08 11:04 UTC\n"
    )


def test_subscription_request_email_falls_back_for_missing_values():
    """Trial signup collects no name and work_domain is nullable — neither may break the render."""
    org, orguser, org_plan = _fake_request_actors(
        work_domain=None, full_name="", role_name=None, base_plan=None
    )
    org.created_at = None

    _, body = build_subscription_request_email(org, orguser, org_plan, REQUESTED_AT)

    assert "  Type:         —\n" in body
    assert "  Created:      —\n" in body
    assert "  Name:         —\n" in body
    assert "  Function:     —\n" in body
    assert "  Dalgo role:   —\n" in body


def test_subscription_request_email_passes_through_unknown_work_domain():
    """A slug added to the form but not yet to WORK_DOMAIN_LABELS must still show up."""
    org, orguser, org_plan = _fake_request_actors(work_domain="brand_new_option")
    _, body = build_subscription_request_email(org, orguser, org_plan, REQUESTED_AT)
    assert "  Function:     brand_new_option\n" in body


def test_subscription_request_email_converts_datetimes_to_utc():
    org, orguser, org_plan = _fake_request_actors()
    ist = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    org.created_at = datetime.datetime(2026, 8, 1, 14, 42, tzinfo=ist)

    _, body = build_subscription_request_email(org, orguser, org_plan, REQUESTED_AT)

    assert "  Created:      2026-08-01 09:12 UTC\n" in body


# ── Internal new-org notification (plain text) ────────────────────────────


def test_new_org_signup_email_renders_both_blocks():
    org, orguser, org_plan = _fake_request_actors()

    subject, body = build_new_org_signup_email(org, orguser, org_plan, org.created_at)

    assert subject == "New org created: Noora Health"
    assert body == (
        "A new org has been created.\n"
        "\n"
        "Org\n"
        "  Name:         Noora Health\n"
        "  Slug:         noora-health\n"
        "  Type:         Free Trial\n"
        "  Created:      2026-08-01 09:12 UTC\n"
        "\n"
        "Signed up by\n"
        "  Name:         Himanshu Dube\n"
        "  Email:        himanshu@projecttech4dev.org\n"
        "  Function:     Monitoring and Evaluation\n"
        "  Dalgo role:   Admin\n"
    )


def test_new_org_signup_email_falls_back_for_missing_values():
    """Trial signup collects no name, and the plan/role lookups can come back empty."""
    org, orguser, _ = _fake_request_actors(work_domain=None, full_name="", role_name=None)

    _, body = build_new_org_signup_email(org, orguser, None, None)

    assert "  Type:         —\n" in body
    assert "  Created:      —\n" in body
    assert "  Name:         —\n" in body
    assert "  Function:     —\n" in body
    assert "  Dalgo role:   —\n" in body


def test_work_domain_labels_cover_the_signup_form_options():
    """Every option a signup can submit must have a label to render."""
    assert set(get_args(WorkDomain)) <= set(WORK_DOMAIN_LABELS)


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


def test_day3_not_started_shows_both_rows_unticked():
    """email A nudges a user who has completed nothing — no ticks, one CTA"""
    plain, html_out = render_trial_day3_not_started_email(
        "https://app.example", "https://cal.example"
    )
    assert "Ready to see Dalgo in action?" in html_out
    assert "Build your first insight" in html_out
    assert "Setup an automated data pipeline" in html_out
    # product_tour is never tracked, so "Explore the platform" must not appear
    assert "Explore the platform" not in html_out
    assert html_out.count("&#10003;") == 0
    assert "OPEN WORKSPACE" in html_out
    assert "Ready to see Dalgo in action?" in plain


def test_day3_in_progress_ticks_completed_flow_and_lists_it_first():
    """email B ticks the finished flow and puts it at the top of the list"""
    plain, html_out = render_trial_day3_in_progress_email(
        "insights", "https://app.example", "https://cal.example"
    )
    assert "Pick up where you left off" in html_out
    assert html_out.count("&#10003;") == 1
    assert html_out.index("Build your first insight") < html_out.index(
        "Setup an automated data pipeline"
    )
    assert "CONTINUE WHERE I LEFT OFF" in html_out
    # the mockup's [first insight/automated pipeline] placeholders are resolved, not literal
    assert "[first insight" not in html_out
    assert "You've built your first insight" in plain
    assert "automated data pipeline" in plain


def test_day3_in_progress_reverses_order_for_the_other_flow():
    """completing the pipeline flow instead puts that row first, ticked"""
    _, html_out = render_trial_day3_in_progress_email(
        "automate_pipeline", "https://app.example", "https://cal.example"
    )
    assert html_out.index("Setup an automated data pipeline") < html_out.index(
        "Build your first insight"
    )
    assert html_out.count("&#10003;") == 1


def test_completion_email_ticks_both_and_offers_keep_exploring():
    """email C congratulates and offers KEEP EXPLORING — there is no upgrade CTA"""
    plain, html_out = render_trial_completion_email("https://app.example", "https://cal.example")
    assert "Congratulations" in html_out
    assert html_out.count("&#10003;") == 2
    assert "KEEP EXPLORING" in html_out
    assert "UPGRADE" not in html_out
    assert "https://app.example" in html_out
    assert "Congratulations" in plain


def test_all_three_carry_testimonial_and_call_link():
    """the testimonial block and footer link are common to A, B and C"""
    renders = [
        render_trial_day3_not_started_email("https://app.example", "https://cal.example"),
        render_trial_day3_in_progress_email(
            "insights", "https://app.example", "https://cal.example"
        ),
        render_trial_completion_email("https://app.example", "https://cal.example"),
    ]
    for _, html_out in renders:
        assert "SEE WHAT'S POSSIBLE" in html_out
        assert "Schedule a call with us" in html_out
        assert "https://cal.example" in html_out
