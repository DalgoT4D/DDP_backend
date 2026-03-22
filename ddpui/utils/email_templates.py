"""HTML email templates for Dalgo notifications"""

import html
from typing import Optional


def _render_thread_html(thread: list) -> str:
    """Render prior comments as HTML thread items."""
    if not thread:
        return ""

    items = []
    for msg in thread:
        safe_name = html.escape(msg["author_name"])
        safe_content = html.escape(msg["content"])
        items.append(
            f'<tr><td style="padding:8px 16px;">'
            f'<p style="margin:0 0 2px; font-size:12px; color:#6b7280; font-weight:600;">{safe_name}</p>'
            f'<p style="margin:0; font-size:13px; color:#374151; line-height:1.4;">{safe_content}</p>'
            f"</td></tr>"
        )

    return (
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="background-color:#f9fafb; border-left:3px solid #d1d5db; border-radius:0 4px 4px 0; margin-bottom:8px;">'
        + "".join(items)
        + "</table>"
    )


def _render_thread_plain(thread: list) -> str:
    """Render prior comments as plain-text thread."""
    if not thread:
        return ""

    lines = []
    for msg in thread:
        lines.append(f'  {msg["author_name"]}: {msg["content"]}')
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
    safe_author_email = html.escape(author_email)
    safe_excerpt = html.escape(comment_excerpt)
    safe_title = html.escape(snapshot_title)
    safe_url = html.escape(report_url)
    safe_chart_name = html.escape(chart_name) if chart_name else None

    thread = thread or []
    thread_plain = _render_thread_plain(thread)
    thread_html = _render_thread_html(thread)

    # Build the "on ..." location string
    if chart_name:
        plain_location = f'"{chart_name}" in "{snapshot_title}"'
        html_location = f'<strong>{safe_chart_name}</strong> in <strong>{safe_title}</strong>'
    else:
        plain_location = f'"{snapshot_title}"'
        html_location = f'<strong>{safe_title}</strong>'

    plain_text = (
        f"{author_name} ({author_email}) mentioned you in a comment on {plain_location}:\n"
        f"\n"
        f"{thread_plain}"
        f"  {author_name}: {comment_excerpt}\n"
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
                {html_location}:
              </p>

              <!-- Thread context (prior comments) -->
              {thread_html}

              <!-- The mention comment (highlighted) -->
              <table width="100%" cellpadding="0" cellspacing="0">
                <tr>
                  <td style="background-color:#f0fdfa; border-left:4px solid #00897B; padding:12px 16px; border-radius:0 4px 4px 0;">
                    <p style="margin:0 0 2px; font-size:12px; color:#00897B; font-weight:600;">{safe_author_name}</p>
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
