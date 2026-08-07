"""Tests for ddpui.utils.email_templates.

Load-bearing goal: after refactoring the outer email shell into a single
``_render_email_shell`` helper, the existing report + mention emails MUST
still render byte-identically to the pre-refactor output. The golden fixtures
under ``fixtures/email_templates/`` were captured from the pre-refactor code
and pinned here — a diff means the shell extraction subtly changed something.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from types import SimpleNamespace

from ddpui.utils import email_templates
from ddpui.utils.email_templates import (
    render_alert_email,
    render_mention_email,
    render_share_report_email,
    render_trial_verification_email,
    render_trial_welcome_email,
)


def _fake_alert(name: str = "High Errors", id_: int = 7) -> SimpleNamespace:
    """A stand-in for a persisted Alert row — render_alert_email only needs .name + .id."""
    return SimpleNamespace(name=name, id=id_)


FIXTURES = Path(__file__).parent / "fixtures" / "email_templates"


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


# ── Trial emails ─────────────────────────────────────────────────────────


def test_render_trial_verification_email_basic_shape():
    plain, html_ = render_trial_verification_email("https://app.dalgo.org/verify?token=abc")

    # Plain-text contract
    assert "https://app.dalgo.org/verify?token=abc" in plain
    assert "24 hours" in plain

    # HTML contract — shell + body
    assert "Dalgo" in html_  # wordmark
    assert "#00897B" in html_  # brand teal from shell
    assert "https://app.dalgo.org/verify?token=abc" in html_
    assert "Verify email" in html_  # CTA label
    assert "24 hours" in html_


def test_render_trial_welcome_email_basic_shape():
    plain, html_ = render_trial_welcome_email("https://app.dalgo.org/login")

    # Plain-text contract
    assert "https://app.dalgo.org/login" in plain

    # HTML contract — shell + body
    assert "Dalgo" in html_
    assert "#00897B" in html_
    assert "https://app.dalgo.org/login" in html_
    assert "Log in" in html_  # CTA label


# ── Shell single-source-of-truth guard ───────────────────────────────────


def test_shell_is_single_source_of_truth():
    """Assert the Dalgo email chrome (teal color + wordmark) is defined in exactly
    one function: ``_render_email_shell``. This is the enforcement mechanism for
    the plan's "one common template" commitment.

    If a future contributor inlines a `#00897B` header or a `<h1>Dalgo</h1>`
    wordmark into another render_* function, this test fails.
    """
    source = Path(email_templates.__file__).read_text()
    module = ast.parse(source)

    # Locate every function definition and its source range.
    fns = {}
    for node in module.body:
        if isinstance(node, ast.FunctionDef):
            fns[node.name] = source.splitlines()[node.lineno - 1 : node.end_lineno]

    assert "_render_email_shell" in fns, "shell helper must exist"

    shell_body = "\n".join(fns["_render_email_shell"])

    # Every render_* function must delegate to _render_email_shell — no inline shell.
    for name, body_lines in fns.items():
        if not name.startswith("render_"):
            continue
        body = "\n".join(body_lines)
        assert (
            "_render_email_shell" in body
        ), f"{name} must call _render_email_shell — do not inline the shell"

    # The teal brand color and the Dalgo wordmark markup live ONLY inside the shell.
    # Anywhere else in the module is fine to reference the color for accents (e.g.
    # a link color) — the ban is specifically on the header/wordmark block.
    wordmark_fragment = '<h1 style="color:#ffffff; margin:0; font-size:18px; font-weight:700; letter-spacing:0.5px;">Dalgo</h1>'
    assert wordmark_fragment in shell_body, "shell must render the Dalgo wordmark"

    # Wordmark appears once in the whole module — only inside _render_email_shell.
    assert (
        source.count(wordmark_fragment) == 1
    ), "Dalgo wordmark HTML fragment must appear in exactly one place — inside _render_email_shell"


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
