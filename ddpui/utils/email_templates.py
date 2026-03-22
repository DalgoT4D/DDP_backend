"""HTML email templates for Dalgo notifications"""

import html


def render_mention_email(
    author_name: str,
    author_email: str,
    comment_excerpt: str,
    snapshot_title: str,
    report_url: str,
) -> tuple:
    """Render HTML and plain-text email for a comment mention notification.

    Returns:
        (plain_text_body, html_body) tuple
    """
    # Escape user-generated content for HTML
    safe_author_name = html.escape(author_name)
    safe_author_email = html.escape(author_email)
    safe_excerpt = html.escape(comment_excerpt)
    safe_title = html.escape(snapshot_title)
    safe_url = html.escape(report_url)

    plain_text = (
        f'{author_name} ({author_email}) mentioned you in a comment on "{snapshot_title}":\n'
        f"\n"
        f'"{comment_excerpt}"\n'
        f"\n"
        f"View the report: {report_url}\n"
        f"\n"
        f"---\n"
        f"You received this email because you were mentioned in a comment on Dalgo.\n"
    )

    html_body = f"""\
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0; padding:0; background-color:#f4f4f5; font-family:Arial, Helvetica, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f4f5; padding:32px 0;">
    <tr>
      <td align="center">
        <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; overflow:hidden;">

          <!-- Header -->
          <tr>
            <td style="background-color:#00897B; padding:24px 32px;">
              <h1 style="color:#ffffff; margin:0; font-size:20px; font-weight:600;">Dalgo</h1>
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="padding:32px;">
              <p style="margin:0 0 16px; font-size:16px; color:#1a1a1a;">
                <strong>{safe_author_name}</strong> ({safe_author_email}) mentioned you in a comment on
                <strong>{safe_title}</strong>:
              </p>

              <!-- Comment excerpt -->
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="background-color:#f4f4f5; border-left:4px solid #00897B; padding:16px; border-radius:0 4px 4px 0;">
                    <p style="margin:0; font-size:14px; color:#374151; line-height:1.5; white-space:pre-wrap;">{safe_excerpt}</p>
                  </td>
                </tr>
              </table>

              <!-- CTA Button -->
              <table width="100%" cellpadding="0" cellspacing="0" style="margin-top:24px;">
                <tr>
                  <td>
                    <a href="{safe_url}"
                       style="display:inline-block; background-color:#00897B; color:#ffffff;
                              padding:12px 24px; text-decoration:none; border-radius:6px;
                              font-size:14px; font-weight:600;">
                      View Report
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:16px 32px; border-top:1px solid #e5e7eb;">
              <p style="margin:0; font-size:12px; color:#9ca3af;">
                You received this email because you were mentioned in a comment on Dalgo.
                You can manage your email notification preferences in your account settings.
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""

    return plain_text, html_body
