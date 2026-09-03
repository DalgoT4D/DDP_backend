"""User-account transactional notification triggers.

Password reset + signup verification stay plaintext — the recipient does not
have a Dalgo account yet at send time, so we cannot templatize per user.

Invite + added-to-org go through the shared notification shell via
``render_notification_email``. Both accept an optional ``group_name`` — when
set (the invite/add originated from a group create or edit flow), the copy
names the group instead of the plain "invited to Dalgo" / "added to org" wording.
"""

import os
from typing import Optional

from ddpui.core.notifications.templates import render_notification_email
from ddpui.utils.awsses import send_html_message, send_text_message


def send_password_reset(to_email: str, reset_url: str) -> None:
    """Password reset email — plain text, sent to the user's current address."""
    message = f"""Hello,

We received a request to reset your Dalgo password.

Please click this link to begin: {reset_url}.

If you did not request a password reset you may safely ignore this email.

"""
    send_text_message(to_email, "You've requested a password reset", message)


def send_signup(to_email: str, verification_url: str) -> None:
    """Signup verification link. Plain text — the recipient does not have an
    account yet, so we cannot templatize per user."""
    message = f"""Hello,

Welcome to Dalgo! Please verify your email address by clicking the link below

{verification_url}

    """
    send_text_message(to_email, "Welcome to Dalgo", message)


def send_invite_user(
    to_email: str,
    invited_by_email: str,
    invite_url: str,
    org_name: Optional[str] = None,
    group_name: Optional[str] = None,
) -> None:
    """Invitation email to a not-yet-registered user.

    CTA is 'Accept Invitation' → invite_url. When ``group_name`` is set, the
    headline names the group and the body says accept + set password to explore
    the workspace."""
    if group_name:
        headline = f"You have been added to the {group_name} group by {invited_by_email}"
        subtext = "Accept the invite and set your password to explore your workspace."
    else:
        dest = org_name or "Dalgo"
        headline = f"You have been invited to {dest} by {invited_by_email}"
        subtext = "Accept the invite and set your password to get started."

    message = f"{subtext}\n{invite_url}"
    plain, html_body = render_notification_email(
        subject=headline, message=message, cta_label="Accept Invitation"
    )
    send_html_message(to_email, headline, plain, html_body)


def send_added_to_org(
    to_email: str,
    added_by: str,
    org_name: str,
    group_name: Optional[str] = None,
) -> None:
    """Notify an existing Dalgo user that they've been added to a new org.

    CTA is 'Explore Workspace' → FRONTEND_URL. When ``group_name`` is set, the
    headline names the group and the body says they inherit access to resources
    shared with that group."""
    explore_url = os.getenv("FRONTEND_URL") or ""
    if group_name:
        headline = f"You have been added to the {group_name} group by {added_by}"
        subtext = (
            f"You now automatically inherit access to all resources shared with "
            f"the {group_name} group."
        )
    else:
        headline = f"You have been added to {org_name} by {added_by}"
        subtext = "You now have access to this Dalgo workspace."

    message = f"{subtext}\n{explore_url}"
    plain, html_body = render_notification_email(
        subject=headline, message=message, cta_label="Explore Workspace"
    )
    send_html_message(to_email, headline, plain, html_body)
