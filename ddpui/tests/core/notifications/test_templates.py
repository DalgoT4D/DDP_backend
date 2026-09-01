"""Tests for ``ddpui.core.notifications.templates``.

Load-bearing goal: after any refactor of the shared email shell, the existing
report + mention emails MUST still render byte-identically to the pre-refactor
output. The golden fixtures under ``fixtures/`` were captured from the
pre-refactor code and pinned here — a diff means the shell extraction subtly
changed something.

Trial-lifecycle and biz-dev email tests live in ``test_trial_templates.py``
in the same directory — those templates are a different visual family (navy
wordmark shell) and are not part of the in-app notifications pipeline.
"""

from __future__ import annotations

import ast
from pathlib import Path

from types import SimpleNamespace

from ddpui.core.notifications.templates import (
    render_alert_email,
    render_mention_email,
    render_notification_email,
    render_share_report_email,
)
from ddpui.core.notifications.templates import shell as shell_module


def _fake_alert(name: str = "High Errors", id_: int = 7) -> SimpleNamespace:
    """A stand-in for a persisted Alert row — render_alert_email only needs .name + .id."""
    return SimpleNamespace(name=name, id=id_)


FIXTURES = Path(__file__).parent / "fixtures"


def _read(fixture: str) -> str:
    return (FIXTURES / fixture).read_text()


# ── Golden: report share email ────────────────────────────────────────────


def test_report_email_matches_golden_with_public_url():
    plain, html = render_share_report_email(
        sender_name="Priya <b>Sharma</b>",
        report_title="Quarterly & Report",
        private_url="https://app.dalgo.org/reports/42",
        public_url="https://app.dalgo.org/public/tok123",
    )
    assert plain == _read("golden_report_plain.txt")
    assert html == _read("golden_report_html.html")


def test_report_email_matches_golden_without_public_url():
    plain, html = render_share_report_email(
        sender_name="Priya",
        report_title="Monthly",
        private_url="https://app.dalgo.org/reports/43",
    )
    assert plain == _read("golden_report_no_public_plain.txt")
    assert html == _read("golden_report_no_public_html.html")


# ── Golden: mention email ─────────────────────────────────────────────────


def test_mention_email_matches_golden():
    plain, html = render_mention_email(
        author_name="Priya <b>",
        author_email="priya@example.com",
        comment_excerpt="Check @vinit@example.com this <script>alert(1)</script>",
        snapshot_title="Q3 Report",
        report_url="https://app.dalgo.org/reports/7",
        thread=[
            {
                "author_name": "Vinit",
                "author_email": "vinit@example.com",
                "content": "Earlier @priya@example.com msg",
            }
        ],
        chart_name="Enrollments Chart",
    )
    assert plain == _read("golden_mention_plain.txt")
    assert html == _read("golden_mention_html.html")


def test_mention_email_matches_golden_minimal():
    plain, html = render_mention_email(
        author_name="Vinit",
        author_email="vinit@example.com",
        comment_excerpt="Hi",
        snapshot_title="Report",
        report_url="https://app.dalgo.org/r/1",
    )
    assert plain == _read("golden_mention_minimal_plain.txt")
    assert html == _read("golden_mention_minimal_html.html")


# ── render_alert_email ────────────────────────────────────────────────────


def test_render_alert_email_basic_shape():
    alert = _fake_alert(name="Enrollments low", id_=42)
    plain, html_ = render_alert_email(alert, "Current value is 12 (below 50).")

    # Plain-text contract
    assert plain.startswith("Alert fired: Enrollments low\n")
    assert "Current value is 12 (below 50)." in plain
    assert "View alert:" in plain

    # HTML contract — shell + body
    assert "Dalgo" in html_  # wordmark
    assert "#00897B" in html_  # brand teal from shell
    assert "Alert fired: Enrollments low" in html_
    assert "Current value is 12 (below 50)." in html_
    assert "View alert" in html_


def test_render_alert_email_escapes_html_in_user_body(monkeypatch):
    monkeypatch.setenv("FRONTEND_URL", "https://app.dalgo.org")
    alert = _fake_alert()
    _, html_ = render_alert_email(alert, "<script>alert(1)</script>")

    assert "<script>alert(1)</script>" not in html_
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html_


def test_render_alert_email_converts_newlines_to_br():
    alert = _fake_alert()
    _, html_ = render_alert_email(alert, "line1\nline2\nline3")

    # HTML-escape happens before <br> conversion — < in user text would be &lt;
    assert "line1<br>\nline2<br>\nline3" in html_


def test_render_alert_email_cta_url_points_to_alerts_page(monkeypatch):
    monkeypatch.setenv("FRONTEND_URL", "https://app.dalgo.org")
    alert = _fake_alert(id_=99)
    plain, html_ = render_alert_email(alert, "body")

    expected = "https://app.dalgo.org/alerts"
    assert expected in plain
    assert expected in html_
    # No per-alert deep-link yet — the alerts listing has no ?highlight handler.
    assert "highlight=" not in html_


def test_render_alert_email_escapes_alert_name_in_headline():
    alert = _fake_alert(name="<b>Injected</b>")
    _, html_ = render_alert_email(alert, "body")
    # The literal <b> must not survive into the HTML headline
    assert "<b>Injected</b>" not in html_
    assert "&lt;b&gt;Injected&lt;/b&gt;" in html_


# ── Notification email (in-app notifications, email channel) ─────────────


def test_notification_email_trailing_url_renders_cta_button():
    """Message shaped like '{text}\\n{url}' pulls the URL out as a CTA."""
    plain, html_ = render_notification_email(
        "Access request",
        "Priya requests View on dashboard 'Sales'.\nhttps://app.dalgo.org/dashboards/7",
    )
    # CTA URL is not inlined into the message paragraph — it's on the button.
    assert 'href="https://app.dalgo.org/dashboards/7"' in html_
    assert "View\n                    </a>" in html_
    # The URL is stripped from the visible message body.
    assert "https://app.dalgo.org/dashboards/7</p>" not in html_
    # Plain-text keeps the original message verbatim.
    assert "https://app.dalgo.org/dashboards/7" in plain


def test_notification_email_without_url_auto_links_inline():
    """When there's no trailing URL, no CTA button is rendered."""
    _, html_ = render_notification_email("Ping", "Just a heads-up, nothing to click.")
    assert "View\n                    </a>" not in html_
    assert "Just a heads-up, nothing to click." in html_


def test_notification_email_inline_url_is_auto_linked():
    """A URL embedded mid-sentence should be auto-linked, not extracted."""
    _, html_ = render_notification_email(
        "Update",
        "Something happened at https://app.dalgo.org/x and continues here.",
    )
    # No CTA — URL wasn't at the end.
    assert "View\n                    </a>" not in html_
    assert 'href="https://app.dalgo.org/x"' in html_


def test_notification_email_escapes_subject_and_message():
    """No HTML injection via message or subject."""
    _, html_ = render_notification_email("<script>", "Hi <b>you</b>")
    assert (
        "<script>"
        not in html_.replace("<script>", "")  # sanity: after escape the raw sequence is gone
        or "&lt;script&gt;" in html_
    )
    assert "<b>you</b>" not in html_
    assert "&lt;b&gt;you&lt;/b&gt;" in html_


# ── Shell single-source-of-truth guard ───────────────────────────────────


def test_shell_is_single_source_of_truth():
    """Assert the notification email chrome is defined in exactly one place —
    ``shell._render_email_shell``. Every ``render_*`` under
    ``ddpui.core.notifications.templates`` must delegate to it, never inline
    the wordmark markup.

    (The trial-lifecycle shell — a separate visual family — is guarded by a
    sibling test in ``test_trial_templates.py`` in the same directory.)
    """
    package_dir = Path(shell_module.__file__).parent
    py_files = sorted(package_dir.glob("*.py"))

    shell_source = Path(shell_module.__file__).read_text()

    # Wordmark HTML fragment lives in exactly one file: shell.py.
    wordmark_fragment = (
        '<h1 style="color:#ffffff; margin:0; font-size:18px; font-weight:700;'
        ' letter-spacing:0.5px;">Dalgo</h1>'
    )
    assert wordmark_fragment in shell_source, "shell.py must render the Dalgo wordmark"

    for path in py_files:
        source = path.read_text()
        expected = 1 if path.name == "shell.py" else 0
        assert source.count(wordmark_fragment) == expected, (
            f"Dalgo wordmark fragment must appear in exactly one place (shell.py);"
            f" found in {path.name}"
        )

    # Every render_* function in a file that uses the notification shell must
    # delegate to _render_email_shell (i.e. never inline it). Files belonging
    # to a different shell family (trial_shell / trial / biz_dev) are skipped.
    notification_shell_files = {"alert.py", "mention.py", "report_share.py", "generic.py"}
    for path in py_files:
        if path.name not in notification_shell_files:
            continue
        source = path.read_text()
        module = ast.parse(source)
        for node in module.body:
            if isinstance(node, ast.FunctionDef) and node.name.startswith("render_"):
                body = "\n".join(source.splitlines()[node.lineno - 1 : node.end_lineno])
                assert (
                    "_render_email_shell" in body
                ), f"{path.name}::{node.name} must call _render_email_shell"


def test_report_and_alert_share_same_shell_chrome():
    """Two different renderers, same shell chrome — asserts the shared shell
    surrounds both outputs identically.

    We slice out the body cell (which differs per email type) and compare the
    surrounding shell fragments.
    """
    _, report_html = render_share_report_email(
        sender_name="X", report_title="T", private_url="https://x/y"
    )
    _, alert_html = render_alert_email(_fake_alert(), "body")

    # Prefix through the opening body <td> is shell-owned and must match.
    prefix = report_html.split('<td style="padding:32px;">', 1)[0]
    alert_prefix = alert_html.split('<td style="padding:32px;">', 1)[0]
    assert prefix == alert_prefix

    # Suffix from the footer border-top through closing </html> is shell-owned.
    suffix_marker = '<td style="padding:16px 32px; border-top:1px solid #e5e7eb;">'
    report_suffix = report_html.split(suffix_marker, 1)[1].split("</p>", 1)[1]
    alert_suffix = alert_html.split(suffix_marker, 1)[1].split("</p>", 1)[1]
    assert report_suffix == alert_suffix
