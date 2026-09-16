"""Generic in-app notification email — the default template used by
``handle_recipient`` when a caller has no specialized renderer.

Callers of ``create_notification`` produce plain-text ``message`` bodies
often shaped like ``"{text}\\n{resource_url}"``. This renderer pulls that
trailing URL out and renders it as a **View** CTA button; inline URLs are
auto-linked.
"""

import html

from ddpui.core.notifications.templates.shell import (
    _render_email_shell,
    escape_and_link_inline,
    split_trailing_url,
)


def render_notification_email(subject: str, message: str, cta_label: str = "View") -> tuple:
    """Wrap an in-app notification in the same visual shell as the report share email.

    - Uses ``email_subject`` as the headline.
    - If the message ends on a URL, pulls it out and renders it as the CTA
      button labeled ``cta_label`` (default "View"). Otherwise auto-links URLs inline.

    Returns ``(plain_text, html_body)``.
    """
    safe_subject = html.escape(subject) if subject else "Dalgo notification"
    safe_cta_label = html.escape(cta_label)
    body_text, cta_url = split_trailing_url(message)
    body_fragment = escape_and_link_inline(body_text) if body_text else ""

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
                      {safe_cta_label}
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
