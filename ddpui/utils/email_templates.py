"""HTML email templates for Dalgo notifications.

All outbound Dalgo emails share the same visual shell — teal header with the
Dalgo wordmark, 600-px container on a light-gray page, and a footer strip. The
shell is defined once in ``_render_email_shell``. Every render_* function in
this module builds its body_html fragment and hands it to the shell. Do not
inline the shell markup anywhere else — the ``test_shell_is_single_source``
test enforces this.
"""

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


# ── In-app notification email ────────────────────────────────────────────


_URL_RE = re.compile(r"(https?://[^\s<>\"']+)")


def _split_trailing_url(message: str) -> tuple:
    """Extract the last URL if it sits on its own trailing line.

    Callers of ``create_notification`` frequently emit messages shaped like
    "{text}\\n{resource_url}". Pulling that trailing URL out lets the email
    template render it as a CTA button instead of inline text.

    Returns ``(body_without_url, cta_url_or_none)``.
    """
    stripped = message.rstrip()
    urls = _URL_RE.findall(stripped)
    if not urls:
        return message, None
    last = urls[-1]
    if stripped.endswith(last):
        head = stripped[: -len(last)].rstrip()
        return head, last
    return message, None


def _escape_and_link_inline(user_body: str) -> str:
    """HTML-escape, wrap URLs in <a>, convert newlines to <br>."""
    escaped = html.escape(user_body)
    linked = _URL_RE.sub(
        r'<a href="\1" style="color:#00897B; text-decoration:underline;">\1</a>',
        escaped,
    )
    return linked.replace("\n", "<br>\n")


def render_notification_email(subject: str, message: str) -> tuple:
    """Wrap an in-app notification in the same visual shell as the report share email.

    Callers of ``create_notification`` produce plain-text ``message`` bodies,
    often with a resource URL on the last line (see e.g. the access-request
    notifications in ``access_api``). This renderer:

    - Uses ``email_subject`` as the headline.
    - If the message ends on a URL, pulls it out and renders it as the CTA
      button ("View"). Otherwise auto-links URLs inline.

    Mirrors the ``render_share_report_email`` chrome for a consistent look
    across Dalgo transactional emails. Returns ``(plain_text, html_body)``.
    """
    safe_subject = html.escape(subject) if subject else "Dalgo notification"
    body_text, cta_url = _split_trailing_url(message)
    body_fragment = _escape_and_link_inline(body_text) if body_text else ""

    plain_text = (
        f"{message}\n"
        f"\n"
        f"---\n"
        f"You received this email because you have Dalgo email notifications enabled.\n"
    )

    cta_block = ""
    if cta_url:
        safe_cta_url = html.escape(cta_url)
        cta_block = f"""

              <!-- CTA Button -->
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td>
                    <a href="{safe_cta_url}"
                       style="display:inline-block; background-color:#00897B; color:#ffffff; padding:10px 24px; text-decoration:none; border-radius:6px; font-size:14px; font-weight:600; letter-spacing:0.3px;">
                      View
                    </a>
                  </td>
                </tr>
              </table>"""

    body_paragraph = ""
    if body_fragment:
        body_paragraph = f"""

              <!-- Message body -->
              <p style="margin:0 0 24px; font-size:14px; color:#374151; line-height:1.6;">
                {body_fragment}
              </p>"""

    body_html = f"""\
              <!-- Headline -->
              <p style="margin:0 0 8px; font-size:17px; color:#111827; font-weight:600; line-height:1.4;">
                {safe_subject}
              </p>{body_paragraph}{cta_block}"""

    return plain_text, _render_email_shell(
        body_html,
        "You received this email because you have Dalgo email notifications enabled.",
    )
