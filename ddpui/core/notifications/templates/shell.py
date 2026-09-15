"""The shared Dalgo email chrome + small HTML primitives.

``_render_email_shell`` is the ONLY place the teal header + Dalgo wordmark +
footer chrome lives. Every body renderer in this package hands its fragment
to the shell; nothing inlines the chrome. ``test_shell_is_single_source``
enforces the invariant.
"""

import html
import re


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


# ── Shared body-fragment primitives (used by more than one renderer) ─────


URL_RE = re.compile(r"(https?://[^\s<>\"']+)")


def escape_and_break(user_body: str) -> str:
    """HTML-escape a user-authored plain-text body, then convert newlines to <br>."""
    return html.escape(user_body).replace("\n", "<br>\n")


def split_trailing_url(message: str) -> tuple:
    """Extract the last URL if it sits on its own trailing line.

    Messages shaped like ``"{text}\\n{resource_url}"`` render nicer when the
    URL becomes a CTA button rather than a line of raw text.

    Returns ``(body_without_url, cta_url_or_none)``.
    """
    stripped = message.rstrip()
    urls = URL_RE.findall(stripped)
    if not urls:
        return message, None
    last = urls[-1]
    if stripped.endswith(last):
        head = stripped[: -len(last)].rstrip()
        return head, last
    return message, None


def escape_and_link_inline(user_body: str) -> str:
    """HTML-escape, wrap URLs in ``<a>``, convert newlines to ``<br>``."""
    escaped = html.escape(user_body)
    linked = URL_RE.sub(
        r'<a href="\1" style="color:#00897B; text-decoration:underline;">\1</a>',
        escaped,
    )
    return linked.replace("\n", "<br>\n")
