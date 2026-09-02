"""Alert-fired email template."""

import html
import os

from ddpui.core.notifications.templates.shell import (
    _render_email_shell,
    escape_and_break,
)


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
    body_fragment = escape_and_break(rendered_body)

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
