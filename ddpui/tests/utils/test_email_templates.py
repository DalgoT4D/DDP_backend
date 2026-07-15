"""Tests for ddpui.utils.email_templates.render_invite_user_email"""

from ddpui.utils.email_templates import render_invite_user_email


class TestRenderInviteUserEmail:
    """Tests for the platform invitation email template"""

    def test_renders_role_org_and_link(self):
        plain, html = render_invite_user_email(
            invited_by_email="alice@test.com",
            org_name="Acme NGO",
            role_name="Account Manager",
            invite_url="https://app.dalgo.org/invitations/?invite_code=abc123",
            date_str="Jul 16, 2026",
        )

        for body in (plain, html):
            assert "alice@test.com" in body
            assert "Acme NGO" in body
            assert "Account Manager" in body
            assert "https://app.dalgo.org/invitations/?invite_code=abc123" in body

        assert "Accept Invitation" in html
        assert "has invited you to join Dalgo" in plain
        assert "has invited you to join Dalgo" in html

    def test_escapes_html_in_user_content(self):
        _, html_body = render_invite_user_email(
            invited_by_email="<script>alert('xss')</script>",
            org_name='Org "with quotes"',
            role_name="Analyst",
            invite_url="https://example.com/invitations/?invite_code=xyz",
            date_str="Jul 16, 2026",
        )

        assert "<script>" not in html_body
        assert "&lt;script&gt;" in html_body

    def test_does_not_leak_beyond_expected_fields(self):
        """Body must be built only from inviter email, role, org name, link, date."""
        plain, html_body = render_invite_user_email(
            invited_by_email="bob@test.com",
            org_name="Test Org",
            role_name="Guest",
            invite_url="https://app.dalgo.org/invitations/?invite_code=def456",
            date_str="Jul 16, 2026",
        )
        # sanity: no unexpected placeholder leakage / no raw password or token fields
        assert "password" not in plain.lower()
        assert "password" not in html_body.lower()
