"""Report-share (PDF-attached) email template."""

import html
from typing import Optional

from ddpui.core.notifications.templates.shell import _render_email_shell


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
