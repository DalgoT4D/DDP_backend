"""HTML email templates for Dalgo notifications.

All outbound Dalgo emails share the same visual shell — teal header with the
Dalgo wordmark, 600-px container on a light-gray page, and a footer strip. The
shell is defined once in ``_render_email_shell``. Every render_* function in
this module builds its body_html fragment and hands it to the shell. Do not
inline the shell markup anywhere else — the ``test_shell_is_single_source``
test enforces this.
"""

import datetime
import html
import os
import re
from typing import Optional


# ── One shell to rule them all ────────────────────────────────────────────


def _render_email_shell(body_html: str, footer_note: str) -> str:
    """Wrap body_html in the shared Dalgo email chrome.

    The shell owns: html/head boilerplate, page padding, the 600-px card,
    the teal header bar with the Dalgo wordmark, the 32-px body cell padding,
    and the footer strip. Callers own everything inside the body cell
    (headline, content, CTA button) via ``body_html``.

    This is the ONLY place the Dalgo email chrome lives. If you find yourself
    copy-pasting `#00897B` or a Dalgo header markup fragment, stop and use
    this helper instead.
    """
    return f"""\
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0; padding:0; background-color:#f4f4f5; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f5; padding:32px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.08);">

          <!-- Header -->
          <tr>
            <td style="background-color:#00897B; padding:20px 32px;">
              <h1 style="color:#ffffff; margin:0; font-size:18px; font-weight:700; letter-spacing:0.5px;">Dalgo</h1>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="padding:32px;">
{body_html}
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:16px 32px; border-top:1px solid #e5e7eb;">
              <p style="margin:0; font-size:12px; color:#9ca3af; line-height:1.5;">
                {footer_note}
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


# ── Comment/mention utilities ─────────────────────────────────────────────


def _strip_mentions(content: str) -> str:
    """Remove @ prefix from email mentions to prevent auto-linking by email clients.

    Converts @user@example.com to user@example.com so email clients
    don't render them as clickable mailto links.
    """
    return re.sub(r"@([\w.+-]+@[\w.-]+\.\w+)", r"\1", content)


def _render_thread_html(thread: list) -> str:
    """Render prior comments as HTML thread items."""
    if not thread:
        return ""

    items = []
    for msg in thread:
        safe_name = html.escape(msg["author_name"])
        safe_content = html.escape(_strip_mentions(msg["content"]))
        items.append(
            f'<tr><td style="padding:10px 16px;'
            f'{" border-top:1px solid #e5e7eb;" if items else ""}">'
            f'<p style="margin:0 0 4px; font-size:13px; color:#6b7280; font-weight:600;">'
            f"{safe_name}</p>"
            f'<p style="margin:0; font-size:14px; color:#374151; line-height:1.5;">'
            f"{safe_content}</p>"
            f"</td></tr>"
        )

    return (
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="background-color:#f9fafb; border-left:3px solid #d1d5db;'
        ' border-radius:0 4px 4px 0; margin-bottom:12px;">' + "".join(items) + "</table>"
    )


def _render_thread_plain(thread: list) -> str:
    """Render prior comments as plain-text thread."""
    if not thread:
        return ""

    lines = []
    for msg in thread:
        lines.append(f'  {msg["author_name"]}: {_strip_mentions(msg["content"])}')
    return "\n".join(lines) + "\n\n"


def render_mention_email(
    author_name: str,
    author_email: str,
    comment_excerpt: str,
    snapshot_title: str,
    report_url: str,
    thread: Optional[list] = None,
    chart_name: Optional[str] = None,
) -> tuple:
    """Render HTML and plain-text email for a comment mention notification.

    Args:
        author_name: Display name of the commenter
        author_email: Email of the commenter
        comment_excerpt: The comment text (truncated)
        snapshot_title: Title of the report snapshot
        report_url: URL to the report
        thread: Optional list of prior comments for context.
                Each item: {"author_name": str, "author_email": str, "content": str}
        chart_name: Optional chart title when comment is on a specific chart

    Returns:
        (plain_text_body, html_body) tuple
    """
    # Escape user-generated content for HTML
    safe_author_name = html.escape(author_name)
    safe_excerpt = html.escape(_strip_mentions(comment_excerpt))
    safe_title = html.escape(snapshot_title)
    safe_url = html.escape(report_url)
    safe_chart_name = html.escape(chart_name) if chart_name else None

    thread = thread or []
    thread_plain = _render_thread_plain(thread)
    thread_html = _render_thread_html(thread)

    # Build the location line (chart name + report title)
    if chart_name:
        location_html = (
            f'<span style="color:#00897B; font-weight:600;">{safe_chart_name}</span>'
            f" &middot; {safe_title}"
        )
        plain_location = f"{chart_name} - {snapshot_title}"
    else:
        location_html = f'<span style="color:#00897B; font-weight:600;">{safe_title}</span>'
        plain_location = snapshot_title

    plain_text = (
        f"{author_name} mentioned you in a comment:\n"
        f"\n"
        f"  Report: {plain_location}\n"
        f"\n"
        f"{thread_plain}"
        f"  {author_name}: {_strip_mentions(comment_excerpt)}\n"
        f"\n"
        f"View the report: {report_url}\n"
        f"\n"
        f"---\n"
        f"You received this email because you were mentioned in a comment on Dalgo.\n"
    )

    body_html = f"""\
              <!-- Headline -->
              <p style="margin:0 0 6px; font-size:17px; color:#111827; font-weight:600; line-height:1.4;">
                {safe_author_name} mentioned you in a comment
              </p>

              <!-- Location badge -->
              <p style="margin:0 0 24px; font-size:14px; color:#6b7280; line-height:1.4;">
                {location_html}
              </p>

              <!-- Thread context (prior comments) -->
              {thread_html}

              <!-- The mention comment (highlighted) -->
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="background-color:#f0fdfa; border-left:4px solid #00897B; padding:12px 16px; border-radius:0 4px 4px 0;">
                    <p style="margin:0 0 4px; font-size:13px; color:#00897B; font-weight:600;">{safe_author_name}</p>
                    <p style="margin:0; font-size:14px; color:#1f2937; line-height:1.5;">{safe_excerpt}</p>
                  </td>
                </tr>
              </table>

              <!-- CTA Button -->
              <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:28px;">
                <tr>
                  <td>
                    <a href="{safe_url}"
                       style="display:inline-block; background-color:#00897B; color:#ffffff;
                              padding:10px 24px; text-decoration:none; border-radius:6px;
                              font-size:14px; font-weight:600; letter-spacing:0.3px;">
                      View Report
                    </a>
                  </td>
                </tr>
              </table>"""

    return plain_text, _render_email_shell(
        body_html,
        "You received this email because you were mentioned in a comment on Dalgo."
        " You can manage your notification preferences in your account settings.",
    )


def render_share_report_email(
    sender_name: str,
    report_title: str,
    private_url: str,
    public_url: Optional[str] = None,
) -> tuple:
    """Render HTML + plain-text email for sharing a report.

    Args:
        sender_name: Display name of the person sharing
        report_title: Title of the report
        private_url: Authenticated URL to view the report (requires login)
        public_url: Optional public URL (included only if report is public)

    Returns:
        (plain_text_body, html_body) tuple
    """
    safe_sender = html.escape(sender_name)
    safe_title = html.escape(report_title)
    safe_private_url = html.escape(private_url)

    # Plain-text version
    plain_text = (
        f'{sender_name} has shared "{report_title}" with you\n'
        f"\n"
        f"Check it out on Dalgo web for the best experience.\n"
        f"\n"
        f"View the report (login required): {private_url}\n"
    )
    if public_url:
        plain_text += f"\nPublic link (no login required): {public_url}\n"
    plain_text += (
        f"\n"
        f"OR\n"
        f"\n"
        f"Download the attached PDF to peruse at your own pace.\n"
        f"\n"
        f"---\n"
        f"You received this email because someone shared a Dalgo report with you.\n"
    )

    # Public link HTML block (only if public)
    public_link_html = ""
    if public_url:
        safe_public_url = html.escape(public_url)
        public_link_html = f"""

              <!-- Public link -->
              <p style="margin:16px 0 0; font-size:13px; color:#6b7280; line-height:1.5;">
                Or view without logging in: <a href="{safe_public_url}" style="color:#00897B; text-decoration:underline;">Public Link</a>
              </p>"""

    body_html = f"""\
              <!-- Headline -->
              <p style="margin:0 0 8px; font-size:17px; color:#111827; font-weight:600; line-height:1.4;">
                {safe_sender} has shared &ldquo;{safe_title}&rdquo; with you &#10024;
              </p>

              <!-- Web experience note -->
              <p style="margin:0 0 24px; font-size:14px; color:#6b7280; line-height:1.5;">
                Check it out on <strong>Dalgo</strong> web for the best experience &#128187;
              </p>

              <!-- CTA Button (private URL - requires login) -->
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td>
                    <a href="{safe_private_url}"
                       style="display:inline-block; background-color:#00897B; color:#ffffff; padding:10px 24px; text-decoration:none; border-radius:6px; font-size:14px; font-weight:600; letter-spacing:0.3px;">
                      View Report
                    </a>
                  </td>
                </tr>
              </table>{public_link_html}

              <!-- OR separator -->
              <p style="margin:20px 0; font-size:13px; color:#9ca3af; text-align:center; font-weight:600;">
                OR
              </p>

              <!-- Attachment note -->
              <p style="margin:0; font-size:13px; color:#6b7280; line-height:1.5;">
                Download attached <strong>PDF</strong> to peruse at your own pace &#128196;
              </p>"""

    return plain_text, _render_email_shell(
        body_html,
        "You received this email because someone shared a Dalgo report with you.",
    )


# ── Free-trial lifecycle emails ───────────────────────────────────────────
#
# This family (verify / welcome / mid-trial / pre-end / post-deletion) uses its
# own shell — a plain white header with the navy "Dalgo" wordmark and an
# optional trial-days badge, not the teal banner from _render_email_shell.
# That's a deliberate visual split, not a duplicate: these are trial-lifecycle
# emails, distinct from the in-product notification emails above. Don't merge
# the two shells — do not inline this one's chrome elsewhere either.

_DALGO_NAVY = "#0F2440"


def _render_trial_header(badge: Optional[str]) -> str:
    """Wordmark + optional trial-days pill, underlined by a thin divider."""
    badge_cell = ""
    if badge:
        safe_badge = html.escape(badge)
        badge_cell = (
            f'<td style="padding-left:12px;"><span style="background-color:#e6f4f1;'
            f" color:#00695c; font-size:13px; font-weight:600; padding:6px 14px;"
            f' border-radius:9999px; white-space:nowrap;">{safe_badge}</span></td>'
        )
    return f"""\
          <tr>
            <td style="padding:24px 32px 20px;">
              <table cellpadding="0" cellspacing="0"><tr>
                <td><span style="color:{_DALGO_NAVY}; font-size:22px; font-weight:800;">Dalgo</span></td>
                {badge_cell}
              </tr></table>
            </td>
          </tr>
          <tr>
            <td style="padding:0 32px;">
              <div style="border-top:1px solid #e5e7eb;"></div>
            </td>
          </tr>"""


def _render_trial_email_shell(body_html: str, badge: Optional[str] = None) -> str:
    """Wrap body_html in the trial-lifecycle shell (white header, navy wordmark).

    See the module note above the ``_DALGO_NAVY`` constant for why this is a
    second, deliberately different shell rather than an extension of
    ``_render_email_shell``.
    """
    header_html = _render_trial_header(badge)
    return f"""\
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0; padding:0; background-color:#f4f4f5; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f5; padding:32px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,0.08);">
{header_html}
          <tr>
            <td style="padding:28px 32px 32px;">
{body_html}
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def _render_trial_cta_button(label: str, url: str, primary: bool = True) -> str:
    """One CTA button — filled teal (primary) or outlined (secondary)."""
    safe_url = html.escape(url)
    safe_label = html.escape(label)
    style = (
        "background-color:#00897B; color:#ffffff; border:1px solid #00897B;"
        if primary
        else "background-color:#ffffff; color:#00897B; border:1px solid #00897B;"
    )
    return (
        f'<a href="{safe_url}" style="display:inline-block; {style} padding:12px 28px;'
        f" text-decoration:none; border-radius:6px; font-size:13px; font-weight:700;"
        f' letter-spacing:0.3px;">{safe_label}</a>'
    )


def _render_trial_action_list(items: list) -> str:
    """The bordered icon/title/subtitle list used by the welcome + mid-trial emails.

    ``items`` is a list of (icon_emoji, title, subtitle) tuples.
    """
    rows = []
    for i, (icon, title, subtitle) in enumerate(items):
        border = "" if i == 0 else "border-top:1px solid #f0f0f0;"
        rows.append(
            f"""
                <tr>
                  <td style="padding:16px; {border}">
                    <table cellpadding="0" cellspacing="0" width="100%"><tr>
                      <td width="44" style="vertical-align:top;">
                        <div style="width:36px; height:36px; border-radius:8px; background-color:#eaf5f2; text-align:center; line-height:36px; font-size:16px;">{icon}</div>
                      </td>
                      <td style="vertical-align:top; padding-left:12px;">
                        <p style="margin:0 0 2px; font-size:15px; font-weight:700; color:#111827;">{html.escape(title)}</p>
                        <p style="margin:0; font-size:13px; color:#6b7280; line-height:1.4;">{html.escape(subtitle)}</p>
                      </td>
                    </tr></table>
                  </td>
                </tr>"""
        )
    return (
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="border:1px solid #e5e7eb; border-radius:10px;">' + "".join(rows) + "</table>"
    )


def _render_trial_progress_bar(day_number: int, total_days: int, danger: bool = False) -> str:
    """The "Day N of total" progress bar. ``danger`` switches the fill from solid
    teal (mid-trial) to a green-to-red gradient (pre-end, "time's running out")."""
    pct = max(0, min(100, round(100 * day_number / total_days)))
    fill_style = (
        "background:linear-gradient(90deg, #22c55e 0%, #f59e0b 65%, #ef4444 100%);"
        if danger
        else "background-color:#00897B;"
    )
    return f"""
              <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:8px;">
                <tr>
                  <td style="font-size:13px; color:#6b7280;">Your trial:</td>
                  <td align="right" style="font-size:13px; color:#111827; font-weight:600;">Day {day_number} of {total_days}</td>
                </tr>
              </table>
              <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px;">
                <tr>
                  <td style="height:8px; border-radius:4px; background-color:#e5e7eb; font-size:0; line-height:0;">
                    <div style="width:{pct}%; height:8px; border-radius:4px; {fill_style}"></div>
                  </td>
                </tr>
              </table>"""


def _render_trial_checklist(items: list) -> str:
    """The circle/tick checklist used by the day-3 and completion emails.

    Each item is ``(done, title, subtitle)``. A done row gets a filled green disc with a tick;
    a pending row gets an empty grey-outlined circle. Distinct from
    ``_render_trial_action_list``, which draws emoji icons inside a bordered box — this list has
    no border and encodes progress, not suggestions.
    """
    rows = []
    for done, title, subtitle in items:
        marker = (
            '<div style="width:26px; height:26px; border-radius:13px; background-color:#16a34a;'
            ' color:#ffffff; text-align:center; line-height:26px; font-size:14px;">&#10003;</div>'
            if done
            else '<div style="width:26px; height:26px; border-radius:13px;'
            ' border:2px solid #d1d5db; box-sizing:border-box;"></div>'
        )
        rows.append(
            f"""
                <tr>
                  <td width="26" valign="top" style="padding:0 14px 20px 0;">{marker}</td>
                  <td valign="top" style="padding:0 0 20px 0;">
                    <div style="font-size:15px; color:#111827; font-weight:700; line-height:1.4;">{html.escape(title)}</div>
                    <div style="font-size:14px; color:#4b5563; line-height:1.5; margin-top:2px;">{html.escape(subtitle)}</div>
                  </td>
                </tr>"""
        )
    return f"""
              <table width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 8px;">{"".join(rows)}
              </table>"""


def _render_trial_testimonial() -> str:
    """The grey "SEE WHAT'S POSSIBLE" quote block. Fixed copy — identical in all three
    progress emails, so it takes no arguments."""
    return """
              <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f8fafc; border-radius:8px; margin:16px 0 24px;">
                <tr>
                  <td style="padding:20px 22px;">
                    <div style="font-size:12px; color:#00897B; font-weight:700; letter-spacing:0.08em; margin-bottom:10px;">SEE WHAT'S POSSIBLE</div>
                    <div style="font-size:15px; color:#111827; font-weight:700; line-height:1.55;">&ldquo;After each day of reporting, we are able to see the reports reflecting the changes automatically. That visibility has brought a lot of meaningful impact to the team&rdquo;</div>
                    <div style="font-size:13px; color:#6b7280; margin-top:10px;">&mdash; Anindita, SNEHA</div>
                  </td>
                </tr>
              </table>"""


def _render_trial_text_link(label: str, url: str) -> str:
    """The teal text link with a trailing up-right arrow, used as the email footer."""
    return (
        f'<a href="{html.escape(url, quote=True)}" style="color:#00897B; font-size:15px;'
        f' font-weight:700; text-decoration:none;">{html.escape(label)} &#8599;</a>'
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


# ── Alert email ──────────────────────────────────────────────────────────


def _escape_and_break(user_body: str) -> str:
    """HTML-escape a user-authored plain-text body, then convert newlines to <br>."""
    return html.escape(user_body).replace("\n", "<br>\n")


def render_alert_email(alert, rendered_body: str) -> tuple:
    """Wrap a Mustache-rendered alert body in the shared Dalgo email shell.

    ``alert`` is a ddpui.models.alert.Alert instance. The user's template body
    (already Mustache-substituted by ddpui.core.alerts.rendering.render) is
    escaped, newlines become ``<br>``, and the result sits inside the Dalgo
    shell with a "View alert" CTA pointing at the alerts listing.

    Slack delivery does NOT use this path — Slack posts get the raw body.
    """
    safe_alert_name = html.escape(alert.name)
    frontend_url = os.getenv("FRONTEND_URL", "").rstrip("/")
    cta_url = f"{frontend_url}/alerts"
    safe_cta_url = html.escape(cta_url)
    body_fragment = _escape_and_break(rendered_body)

    plain_text = (
        f"Alert fired: {alert.name}\n"
        f"\n"
        f"{rendered_body}\n"
        f"\n"
        f"View alert: {cta_url}\n"
        f"\n"
        f"---\n"
        f"You received this email because you are a recipient on this Dalgo alert.\n"
    )

    body_html = f"""\
              <!-- Headline -->
              <p style="margin:0 0 8px; font-size:17px; color:#111827; font-weight:600; line-height:1.4;">
                Alert fired: {safe_alert_name}
              </p>

              <!-- User-authored body -->
              <p style="margin:0 0 24px; font-size:14px; color:#374151; line-height:1.6;">
                {body_fragment}
              </p>

              <!-- CTA Button -->
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td>
                    <a href="{safe_cta_url}"
                       style="display:inline-block; background-color:#00897B; color:#ffffff; padding:10px 24px; text-decoration:none; border-radius:6px; font-size:14px; font-weight:600; letter-spacing:0.3px;">
                      View alert
                    </a>
                  </td>
                </tr>
              </table>"""

    return plain_text, _render_email_shell(
        body_html,
        "You received this email because you are a recipient on this Dalgo alert.",
    )


# ── Internal (team-facing) plain-text notifications ───────────────────────
#
# Everything above renders customer-facing HTML through the Dalgo shell. The
# helpers below render PLAIN TEXT for the internal team — no shell, no chrome,
# no branding — because they are read as a work item, not as a Dalgo email.


# labels for OrgUser.work_domain (the signup form's "Function" pick — trial_schema.WorkDomain).
# An unknown slug falls through to the raw value rather than being dropped, so a row not yet
# moved by `manage.py migrate_work_domains` still renders.
WORK_DOMAIN_LABELS = {
    "monitoring_evaluation": "Monitoring and Evaluation",
    "program_implementation": "Program Implementation",
    "data_technology": "Data and Technology",
    "leadership": "Leadership (Founder, COO, CTO, etc.)",
    "external_consultant": "External Consultant",
}

# rendered in place of any field the DB does not have a value for
_MISSING = "—"


def _fmt_datetime_utc(value) -> str:
    """Render a datetime as `YYYY-MM-DD HH:MM UTC`, or the missing-value dash when None.

    Aware datetimes are converted to UTC first; a naive one is assumed to already be UTC
    (USE_TZ is on, so naive values should not occur — this is belt-and-braces so a stray
    naive value formats instead of raising).
    """
    if value is None:
        return _MISSING
    if value.tzinfo is not None:
        value = value.astimezone(datetime.timezone.utc)
    return value.strftime("%Y-%m-%d %H:%M UTC")


def build_subscription_request_email(org, orguser, org_plan, requested_at) -> tuple:
    """Render the internal notification for a "request a subscription" click.

    Plain text, addressed to the partnerships/biz-dev team (BIZ_DEV_EMAILS) — deliberately
    just who asked and which org, so it can be actioned without opening the admin. Every
    value comes from the DB; nothing here is caller-supplied.

    "Type" comes from `org_plan.base_plan` (Free Trial / Dalgo / Internal) — the Org model
    itself has had no `type` column since migration 0093, and the plan is what actually
    distinguishes a trial org from a paying one.

    `orguser.work_domain` is the job title the user self-selected at signup — metadata only,
    NOT a permission. `orguser.new_role` is the actual Dalgo RBAC role. Both are shown
    because they answer different questions ("who is this person" vs "what can they do").

    Returns:
        (subject, plain_text_body) tuple
    """
    subject = f"Subscription request: {org.name}"

    user = orguser.user
    full_name = user.get_full_name().strip() if user else ""
    work_domain = orguser.work_domain
    role = orguser.new_role

    body = (
        "Org\n"
        f"  Name:         {org.name or _MISSING}\n"
        f"  Slug:         {org.slug or _MISSING}\n"
        f"  Type:         {(org_plan.base_plan if org_plan else None) or _MISSING}\n"
        f"  Created:      {_fmt_datetime_utc(org.created_at)}\n"
        "\n"
        "Requested by\n"
        f"  Name:         {full_name or _MISSING}\n"
        f"  Email:        {user.email if user else _MISSING}\n"
        f"  Function:     {WORK_DOMAIN_LABELS.get(work_domain, work_domain) or _MISSING}\n"
        f"  Dalgo role:   {role.name if role else _MISSING}\n"
        f"  Requested at: {_fmt_datetime_utc(requested_at)}\n"
    )

    return subject, body
