"""Trial-lifecycle email renderers.

Sits atop ``trial_shell`` — every renderer here delegates to
``_render_trial_email_shell`` for chrome; the file owns only the per-stage
body copy + CTA wiring.
"""

import html

from ddpui.core.notifications.templates.trial_shell import (
    _render_trial_action_list,
    _render_trial_checklist,
    _render_trial_cta_button,
    _render_trial_email_shell,
    _render_trial_progress_bar,
    _render_trial_testimonial,
    _render_trial_text_link,
)


# Checklist copy for the two tracked walkthrough flows, keyed by the flow name used in
# UserPreferences.trial_walkthrough. product_tour is deliberately absent — it is not tracked by
# any lifecycle email and must never appear as a checklist row.
TRIAL_FLOW_COPY = {
    "insights": (
        "Build your first insight",
        "Build out your first dashboard and share it",
    ),
    "automate_pipeline": (
        "Setup an automated data pipeline",
        "Setup your data to be updated, cleaned and computed regularly",
    ),
}

# Short forms used in email B's subhead, e.g. "You've built your first insight. Next, go
# ahead with your automated data pipeline."
_TRIAL_FLOW_SHORT = {
    "insights": "first insight",
    "automate_pipeline": "automated data pipeline",
}


def _trial_footer_html(schedule_call_url: str) -> str:
    """Testimonial block plus the schedule-a-call link — the shared tail of emails A, B and C."""
    return _render_trial_testimonial() + _render_trial_text_link(
        "Schedule a call with us", schedule_call_url
    )


def render_verify_email(verification_url: str) -> tuple:
    """Template 1 — sent right after signup, before the account is usable.

    Returns:
        (plain_text_body, html_body) tuple
    """
    plain_text = (
        f"Welcome to Dalgo\n"
        f"\n"
        f"You're one step away from turning your programme data into live and"
        f" actionable insights.\n"
        f"\n"
        f"Verify your email: {verification_url}\n"
        f"\n"
        f"This link expires in 24 hours.\n"
    )

    body_html = f"""\
              <p style="margin:0 0 8px; font-size:24px; color:#111827; font-weight:800; line-height:1.3;">
                Welcome to Dalgo
              </p>
              <p style="margin:0 0 28px; font-size:15px; color:#4b5563; line-height:1.6;">
                You're one step away from turning your programme data into live and actionable insights.
              </p>
              {_render_trial_cta_button("VERIFY EMAIL", verification_url)}
              <p style="margin:20px 0 0; font-size:13px; color:#9ca3af;">
                This link expires in 24 hours.
              </p>"""

    return plain_text, _render_trial_email_shell(body_html)


def render_trial_welcome_email(workspace_url: str, trial_days: int = 14) -> tuple:
    """Template 2 — sent once email is verified and the trial workspace is provisioned.

    Args:
        workspace_url: link into the freshly-provisioned trial workspace
        trial_days: total length of the trial (for the "Trial · N days" badge)

    Returns:
        (plain_text_body, html_body) tuple
    """
    actions = [
        ("\U0001f5fa️", "Explore the platform", "Take a quick tour of Dalgo's capabilities"),
        ("\U0001f4ca", "Build your first insight", "Build out your first dashboard and share it"),
        (
            "\U0001f517",
            "Build an automated data pipeline",
            "Setup your data to be updated, cleaned and computed daily/weekly",
        ),
    ]

    plain_text = (
        f"Your workspace is ready\n"
        f"\n"
        f"It comes preloaded with sample data, so you can see what Dalgo does without"
        f" connecting anything first. Here are three ways to start, most people begin"
        f" at the top.\n"
        f"\n" + "\n".join(f"- {title}: {subtitle}" for _, title, subtitle in actions) + f"\n\n"
        f"Open your workspace: {workspace_url}\n"
    )

    body_html = f"""\
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                Your workspace is ready
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                It comes preloaded with sample data, so you can see what Dalgo does without connecting anything first. Here are three ways to start, most people begin at the top.
              </p>
              {_render_trial_action_list(actions)}
              <div style="margin-top:24px;">
                {_render_trial_cta_button("OPEN MY WORKSPACE", workspace_url)}
              </div>"""

    return plain_text, _render_trial_email_shell(body_html, badge=f"Trial · {trial_days} days")


def render_trial_midpoint_email(day_number: int, total_days: int, schedule_call_url: str) -> tuple:
    """Template 3 — mid-trial nudge (e.g. day 7 of 14).

    Returns:
        (plain_text_body, html_body) tuple
    """
    days_left = total_days - day_number
    actions = [
        (
            "\U0001f514",
            "Set up an alert",
            "Get notified when a number crosses a line you care about.",
        ),
        ("\U0001f4c4", "Create a report", "Turn your dashboards into funder-ready reports."),
        (
            "\U0001f4c8",
            "Explore metrics",
            "Define a number once and reuse it across every KPI and chart.",
        ),
    ]

    plain_text = (
        f"You're halfway through your trial period\n"
        f"\n"
        f"Day {day_number} of {total_days}\n"
        f"\n"
        f"You've got {days_left} days left to see what Dalgo can do for your programme."
        f" Many NGOs run their whole M&E on Dalgo, we'd love for yours to be one of them."
        f" Explore other functionality offered by Dalgo\n"
        f"\n" + "\n".join(f"- {title}: {subtitle}" for _, title, subtitle in actions) + f"\n\n"
        f"Schedule a call: {schedule_call_url}\n"
    )

    body_html = f"""\
              {_render_trial_progress_bar(day_number, total_days)}
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                You're halfway through your trial period
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                You've got {days_left} days left to see what Dalgo can do for your programme. Many NGOs run their whole M&E on Dalgo, we'd love for yours to be one of them. Explore other functionality offered by Dalgo
              </p>
              {_render_trial_action_list(actions)}
              <div style="margin-top:24px;">
                {_render_trial_cta_button("SCHEDULE A CALL", schedule_call_url)}
              </div>"""

    return plain_text, _render_trial_email_shell(body_html, badge=f"Trial · {days_left} days left")


def render_trial_pre_end_email(
    day_number: int,
    total_days: int,
    end_date: str,
    schedule_call_url: str,
) -> tuple:
    """Template 4 — pre-end warning (e.g. day 12 of 14, "2 days left").

    Args:
        end_date: human-readable trial end date, already formatted by the caller
            (e.g. "15 Aug 2026") — this function doesn't do date math.

    Returns:
        (plain_text_body, html_body) tuple
    """
    days_left = total_days - day_number
    safe_end_date = html.escape(end_date)

    plain_text = (
        f"{days_left} days left in your trial\n"
        f"\n"
        f"Day {day_number} of {total_days}\n"
        f"\n"
        f"Your trial ends on {end_date}, following which your workspace and its data"
        f" will be permanently deleted. To keep working on Dalgo, schedule a call"
        f" with us.\n"
        f"\n"
        f"Schedule a call: {schedule_call_url}\n"
    )

    body_html = f"""\
              {_render_trial_progress_bar(day_number, total_days, danger=True)}
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                {days_left} days left in your trial
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                Your trial ends on {safe_end_date}, following which your workspace and its data will be permanently deleted. To keep working on Dalgo, schedule a call with us.
              </p>
              {_render_trial_cta_button("SCHEDULE A CALL", schedule_call_url)}"""

    return plain_text, _render_trial_email_shell(body_html, badge=f"Trial · {days_left} days left")


def render_trial_post_deletion_email(
    schedule_call_url: str,
    testimonial_quote: str = (
        "The team has also been extremely supportive, both in helping us make sense of"
        " our data, and also in training us to independently use the platform"
    ),
    testimonial_author: str = "Executive Assistant to the Co-founder, BHUMI",
) -> tuple:
    """Template 5 — sent once the trial workspace has actually been deleted.

    Returns:
        (plain_text_body, html_body) tuple
    """
    safe_quote = html.escape(testimonial_quote)
    safe_author = html.escape(testimonial_author)

    plain_text = (
        f"Thanks for building with us\n"
        f"\n"
        f"Your trial has ended, you will no longer be able to access your workspace as"
        f" it has been deleted.\n"
        f"\n"
        f"If you wish to setup a permanent account, provision another trial, or share"
        f" your experience with us, we'd love to chat.\n"
        f"\n"
        f"Schedule a call: {schedule_call_url}\n"
        f"\n"
        f'"{testimonial_quote}"\n'
        f"— {testimonial_author}\n"
    )

    body_html = f"""\
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                Thanks for building with us
              </p>
              <p style="margin:0 0 16px; font-size:15px; color:#4b5563; line-height:1.6;">
                Your trial has ended, you will no longer be able to access your workspace as it has been deleted.
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                If you wish to setup a permanent account, provision another trial, or share your experience with us, we'd love to chat.
              </p>
              <div style="margin-bottom:28px;">
                {_render_trial_cta_button("SCHEDULE A CALL", schedule_call_url)}
              </div>
              <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f9fafb; border-radius:8px;">
                <tr>
                  <td style="padding:20px 24px;">
                    <p style="margin:0 0 10px; font-size:12px; color:#00897B; font-weight:700; letter-spacing:0.5px;">WHY TEAMS STAY</p>
                    <p style="margin:0 0 10px; font-size:15px; color:#111827; font-weight:700; line-height:1.5;">&ldquo;{safe_quote}&rdquo;</p>
                    <p style="margin:0; font-size:13px; color:#6b7280;">&mdash; {safe_author}</p>
                  </td>
                </tr>
              </table>"""

    return plain_text, _render_trial_email_shell(body_html)


def render_trial_day3_not_started_email(workspace_url: str, schedule_call_url: str) -> tuple:
    """Template A — day 3, no walkthrough completed yet.

    Returns:
        (plain_text_body, html_body) tuple
    """
    items = [(False, *TRIAL_FLOW_COPY[flow]) for flow in ("insights", "automate_pipeline")]

    plain_text = (
        "Ready to see Dalgo in action?\n"
        "\n"
        "Your workspace is setup. Try out one of these guides to get started on Dalgo today\n"
        "\n" + "\n".join(f"- {title}: {subtitle}" for _, title, subtitle in items) + "\n\n"
        f"Open your workspace: {workspace_url}\n"
        f"Schedule a call with us: {schedule_call_url}\n"
    )

    body_html = f"""\
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                Ready to see Dalgo in action?
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                Your workspace is setup. Try out one of these guides to get started on Dalgo today
              </p>
              {_render_trial_checklist(items)}
              {_render_trial_cta_button("OPEN WORKSPACE", workspace_url)}
              {_trial_footer_html(schedule_call_url)}"""

    return plain_text, _render_trial_email_shell(body_html)


def render_trial_day3_in_progress_email(
    completed_flow: str, workspace_url: str, schedule_call_url: str
) -> tuple:
    """Template B — day 3, exactly one walkthrough completed.

    Args:
        completed_flow: "insights" or "automate_pipeline" — the one already finished. It is
            ticked and listed first; the other is the one the copy points at next.

    Returns:
        (plain_text_body, html_body) tuple
    """
    remaining_flow = "automate_pipeline" if completed_flow == "insights" else "insights"
    items = [
        (True, *TRIAL_FLOW_COPY[completed_flow]),
        (False, *TRIAL_FLOW_COPY[remaining_flow]),
    ]
    subhead = (
        f"You've built your {_TRIAL_FLOW_SHORT[completed_flow]}."
        f" Next, go ahead with your {_TRIAL_FLOW_SHORT[remaining_flow]}"
    )

    plain_text = (
        "Pick up where you left off\n"
        "\n"
        f"{subhead}\n"
        "\n" + "\n".join(f"- {title}: {subtitle}" for _, title, subtitle in items) + "\n\n"
        f"Continue where you left off: {workspace_url}\n"
        f"Schedule a call with us: {schedule_call_url}\n"
    )

    body_html = f"""\
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                Pick up where you left off
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                {html.escape(subhead)}
              </p>
              {_render_trial_checklist(items)}
              {_render_trial_cta_button("CONTINUE WHERE I LEFT OFF", workspace_url)}
              {_trial_footer_html(schedule_call_url)}"""

    return plain_text, _render_trial_email_shell(body_html)


def render_trial_completion_email(workspace_url: str, schedule_call_url: str) -> tuple:
    """Template C — both walkthroughs completed, on or after day 3.

    Returns:
        (plain_text_body, html_body) tuple
    """
    items = [(True, *TRIAL_FLOW_COPY[flow]) for flow in ("insights", "automate_pipeline")]

    plain_text = (
        "Congratulations you've completed your tour of Dalgo.\n"
        "\n"
        "Talk to us or explore the platform further.\n"
        "\n" + "\n".join(f"- {title}: {subtitle}" for _, title, subtitle in items) + "\n\n"
        f"Keep exploring: {workspace_url}\n"
        f"Schedule a call with us: {schedule_call_url}\n"
    )

    body_html = f"""\
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                Congratulations you've completed your tour of Dalgo.
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                Talk to us or explore the platform further.
              </p>
              {_render_trial_checklist(items)}
              {_render_trial_cta_button("KEEP EXPLORING", workspace_url)}
              {_trial_footer_html(schedule_call_url)}"""

    return plain_text, _render_trial_email_shell(body_html)
