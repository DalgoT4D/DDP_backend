"""Schemas for the free-trial signup → activate → clone flow.

Request payloads for the public trial endpoints (`ddpui/api/trial_api.py`) plus the typed
shapes passed between the trial core modules (activation-token / clone-params redis blobs,
provisioned-warehouse connection params, and the clone_template_org input).
"""

from typing import Literal, Optional

from ninja import Schema

# the "Function" options both signup forms offer — this one and the post-invitation signup
# (AcceptInvitationSchema.work_domain). Stored on OrgUser.work_domain / TrialSignup.role:
# metadata only, never an RBAC role_slug. Old rows still hold retired slugs; move them with
# `manage.py migrate_work_domains`.
WorkDomain = Literal[
    "monitoring_evaluation",
    "program_implementation",
    "data_technology",
    "leadership",
    "external_consultant",
]


class TrialSignupSchema(Schema):
    """payload for POST /trial/signup"""

    email: str
    org_name: str
    # the "Function" pick on the form; only the current options are accepted, retired slugs
    # (still present in the DB on older rows) are not
    role: WorkDomain


class TrialActivateSchema(Schema):
    """payload for POST /trial/activate"""

    token: str
    password: str


class TrialValidatePasswordSchema(Schema):
    """payload for POST /trial/validate-password.

    Deliberately password-only: this endpoint runs Django's validators and nothing else, so it
    needs no activation token and touches no state.
    """

    password: str


class ActivationTokenData(Schema):
    """what an activation token stores in redis: the signup form fields, replayed at
    /trial/activate once the user clicks the emailed verification link.

    `role` is the job-title captured on the signup form — metadata only, NEVER an RBAC
    role_slug (client-supplied; see `clone_trial_org_task`). Left as a plain str on purpose: a
    token minted before the option list changed must still activate, not 422.
    """

    email: str
    org_name: str
    role: str


class TrialCloneParams(Schema):
    """clone params stashed in redis per task_id at /trial/activate, so POST /trial/retry
    can re-enqueue the clone after a failure without the (already-consumed) activation token."""

    email: str
    org_name: str
    role: str
    template_org_id: int


class TrialDbParams(Schema):
    """connection params for a freshly-provisioned trial warehouse database, using the
    FT-USER's own credentials (never the admin/master credentials)."""

    host: str
    port: int
    database: str
    username: str
    password: str


class TrialCloneRequest(Schema):
    """input to `clone_template_org` — one template→trial clone run.

    `org_name`/`role_slug` are optional overrides applied in Step 1; when None the template's
    name / ACCOUNT_MANAGER_ROLE are used (see `_step_org_and_user`).

    `work_domain` is the job-title the user picked on the signup form (see the `role` field
    above). It is stored as plain metadata on the created OrgUser and is NEVER an RBAC role —
    keep it strictly separate from `role_slug`.
    """

    template_org_id: int
    trial_email: str
    org_name: Optional[str] = None
    role_slug: Optional[str] = None
    work_domain: Optional[str] = None
