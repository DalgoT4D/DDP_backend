"""Trial-lifecycle email shell + component helpers.

This family (verify / welcome / mid-trial / pre-end / post-deletion) uses a
different visual shell than the in-product notification shell — a plain
white header with the navy "Dalgo" wordmark and an optional trial-days
badge, versus the teal banner in ``shell._render_email_shell``.

Kept as a separate shell on purpose: these are onboarding / lifecycle
emails, distinct from the in-app notifications pipeline. Don't merge the
two shells — do not inline this one's chrome elsewhere either.
"""

import html
from typing import Optional


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

    See the module docstring for why this is a second, deliberately different
    shell rather than an extension of ``shell._render_email_shell``.
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
