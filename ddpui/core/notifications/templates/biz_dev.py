"""Internal team-facing plain-text notifications.

Rendered as PLAIN TEXT for the biz-dev/partnerships team — no shell, no
chrome, no branding — because these are read as work items, not as Dalgo
customer emails.
"""

import datetime


# labels for OrgUser.work_domain (the signup form's "Function" pick — trial_schema.WorkDomain).
# An unknown slug falls through to the raw value rather than being dropped, so a row not yet
# moved by `manage.py migrate_work_domains` still renders.
WORK_DOMAIN_LABELS = {
    "monitoring_evaluation": "Monitoring and Evaluation",
    "program_implementation": "Program Implementation",
    "data_technology": "Data and Technology",
    "leadership": "Leadership (Founder, COO, CTO, etc.)",
    "external_consultant": "External Consultant",
}

# rendered in place of any field the DB does not have a value for
_MISSING = "—"


def _fmt_datetime_utc(value) -> str:
    """Render a datetime as `YYYY-MM-DD HH:MM UTC`, or the missing-value dash when None.

    Aware datetimes are converted to UTC first; a naive one is assumed to already be UTC
    (USE_TZ is on, so naive values should not occur — this is belt-and-braces so a stray
    naive value formats instead of raising).
    """
    if value is None:
        return _MISSING
    if value.tzinfo is not None:
        value = value.astimezone(datetime.timezone.utc)
    return value.strftime("%Y-%m-%d %H:%M UTC")


def build_subscription_request_email(org, orguser, org_plan, requested_at) -> tuple:
    """Render the internal notification for a "request a subscription" click.

    Plain text, addressed to the partnerships/biz-dev team (BIZ_DEV_EMAILS) — deliberately
    just who asked and which org, so it can be actioned without opening the admin. Every
    value comes from the DB; nothing here is caller-supplied.

    "Type" comes from `org_plan.base_plan` (Free Trial / Dalgo / Internal) — the Org model
    itself has had no `type` column since migration 0093, and the plan is what actually
    distinguishes a trial org from a paying one.

    `orguser.work_domain` is the job title the user self-selected at signup — metadata only,
    NOT a permission. `orguser.new_role` is the actual Dalgo RBAC role. Both are shown
    because they answer different questions ("who is this person" vs "what can they do").

    Returns:
        (subject, plain_text_body) tuple
    """
    subject = f"Subscription request: {org.name}"

    user = orguser.user
    full_name = user.get_full_name().strip() if user else ""
    work_domain = orguser.work_domain
    role = orguser.new_role

    body = (
        "Org\n"
        f"  Name:         {org.name or _MISSING}\n"
        f"  Slug:         {org.slug or _MISSING}\n"
        f"  Type:         {(org_plan.base_plan if org_plan else None) or _MISSING}\n"
        f"  Created:      {_fmt_datetime_utc(org.created_at)}\n"
        "\n"
        "Requested by\n"
        f"  Name:         {full_name or _MISSING}\n"
        f"  Email:        {user.email if user else _MISSING}\n"
        f"  Function:     {WORK_DOMAIN_LABELS.get(work_domain, work_domain) or _MISSING}\n"
        f"  Dalgo role:   {role.name if role else _MISSING}\n"
        f"  Requested at: {_fmt_datetime_utc(requested_at)}\n"
    )

    return subject, body


def build_new_org_signup_email(org, orguser, org_plan, created_at) -> tuple:
    """Render the internal "a new org has been created" notification.

    Same audience and same plain-text shape as `build_subscription_request_email` — the
    biz-dev team (BIZ_DEV_EMAILS) reads both in the same inbox, so they are kept visually
    consistent on purpose. Sent once, when the org actually exists (the trial clone finished
    all its steps), not when the signup form is submitted: a signup whose clone failed is torn
    down and has no org to talk about.

    `orguser.work_domain` is the "Function" the user picked on the signup form — metadata, not
    a permission — and `orguser.new_role` is the RBAC role the clone assigned. Both are shown,
    as in the subscription email, because they answer different questions.

    Returns:
        (subject, plain_text_body) tuple
    """
    subject = f"New org created: {org.name}"

    user = orguser.user if orguser else None
    full_name = user.get_full_name().strip() if user else ""
    work_domain = orguser.work_domain if orguser else None
    role = orguser.new_role if orguser else None

    body = (
        "A new org has been created.\n"
        "\n"
        "Org\n"
        f"  Name:         {org.name or _MISSING}\n"
        f"  Slug:         {org.slug or _MISSING}\n"
        f"  Type:         {(org_plan.base_plan if org_plan else None) or _MISSING}\n"
        f"  Created:      {_fmt_datetime_utc(created_at)}\n"
        "\n"
        "Signed up by\n"
        f"  Name:         {full_name or _MISSING}\n"
        f"  Email:        {user.email if user else _MISSING}\n"
        f"  Function:     {WORK_DOMAIN_LABELS.get(work_domain, work_domain) or _MISSING}\n"
        f"  Dalgo role:   {role.name if role else _MISSING}\n"
    )

    return subject, body
