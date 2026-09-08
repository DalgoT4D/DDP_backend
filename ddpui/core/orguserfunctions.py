"""
functions to work with OrgUsers
do not raise http errors here
"""

import os
from datetime import datetime
from uuid import uuid4

from django.contrib.auth.models import User
from django.db import transaction
from django.utils.text import slugify
from django.utils import timezone as django_timezone

from ddpui.auth import ACCOUNT_MANAGER_ROLE, GUEST_ROLE, user_has_platform_admin_permission
from ddpui.models.alert import Alert
from ddpui.models.dashboard import Dashboard
from ddpui.models.metric import KPI, Metric
from ddpui.models.org import Org, OrgType
from ddpui.models.report import ReportSnapshot
from ddpui.models.visualization import Chart
from ddpui.models.org_user import (
    AcceptInvitationSchema,
    DeleteOrgUserPayload,
    Invitation,
    NewInvitationSchema,
    OrgUser,
    OrgUserCreate,
    OrgUserUpdate,
    OrgUserUpdatev1,
    ResetPasswordSchema,
    ChangePasswordSchema,
    UserAttributes,
    VerifyEmailSchema,
    OrgUserRole,
)
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.orgtnc import OrgTnC
from ddpui.models.role_based_access import Role
from ddpui.core.notifications.triggers import user as user_notifications
from ddpui.utils import helpers, timezone
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.orguserhelpers import from_invitation, from_orguser
from ddpui.utils.redis_client import RedisClient

logger = CustomLogger("ddpui")


def lookup_user(email: str):
    """look up user by username"""
    user = User.objects.filter(email=email).first()

    if user is None:
        logger.error(f"lookup_user: User with email '{email}' not found in database")
        raise Exception(f"User with email '{email}' not found")

    userattributes = UserAttributes.objects.filter(user=user).first()
    if userattributes is None:
        userattributes = UserAttributes.objects.create(user=user)

    email_verified = userattributes.email_verified
    if email_verified is False:
        # check if all the orgusers for this user have email verified
        email_verified = OrgUser.objects.filter(user=user, email_verified=True).exists()
        if email_verified:
            userattributes.email_verified = True
            userattributes.save()
            # to be removed soon
            OrgUser.objects.filter(user=user, email_verified=False).update(
                email_verified=True, updated_at=django_timezone.now()
            )

    return {
        "email": user.email,
        "email_verified": userattributes.email_verified,
        "active": user.is_active,
        "can_create_orgs": userattributes.can_create_orgs,
        "is_consultant": userattributes.is_consultant,
        # from the user's roles, not UserAttributes: no org context here, so "in some org"
        "is_platform_admin": user_has_platform_admin_permission(user),
    }


def create_orguser(payload: OrgUserCreate, email_verified: bool = False) -> OrgUser:
    """create the user and orguser"""
    signupcode = payload.signupcode
    if signupcode not in [os.getenv("SIGNUPCODE"), os.getenv("DEMO_SIGNUPCODE")]:
        raise Exception("That is not the right signup code")

    if User.objects.filter(email=payload.email).exists():
        raise Exception(f"user having email {payload.email} exists")

    if User.objects.filter(username=payload.email).exists():
        raise Exception(f"user having email {payload.email} exists")

    if not helpers.isvalid_email(payload.email):
        raise Exception("that is not a valid email address")

    is_demo = True if (signupcode == os.getenv("DEMO_SIGNUPCODE")) else False
    demo_org = None  # common demo org
    if is_demo:
        demo_org = Org.objects.filter(type=OrgType.DEMO).first()
        if demo_org is None:
            raise Exception("demo org has not been setup")

    user = User.objects.create_user(
        username=payload.email, email=payload.email, password=payload.password
    )
    UserAttributes.objects.create(user=user, email_verified=email_verified)
    new_role = None
    if is_demo:
        new_role = Role.objects.filter(slug=GUEST_ROLE).first()
    else:
        new_role = Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()

    orguser = OrgUser.objects.create(
        user=user,
        org=demo_org,
        new_role=new_role,
        email_verified=email_verified,
    )
    orguser.save()
    UserPreferences.objects.create(orguser=orguser, enable_email_notifications=True)
    logger.info(
        f"created user {new_role.slug} " f"{orguser.user.email} having userid {orguser.user.id}"
    )

    return orguser


def signup_orguser(payload: OrgUserCreate):
    """create an orguser and send an email"""

    try:
        orguser = create_orguser(payload)
    except Exception as err:
        logger.exception(err)
        return None, str(err)

    redis = RedisClient.get_instance()
    token = uuid4()

    redis_key = f"email-verification:{token.hex}"
    orguserid_bytes = str(orguser.id).encode("utf8")

    redis.set(redis_key, orguserid_bytes)

    FRONTEND_URL = os.getenv("FRONTEND_URL")
    reset_url = f"{FRONTEND_URL}/verifyemail/?token={token.hex}"
    try:
        user_notifications.send_signup(payload.email, reset_url)
    except Exception:
        return None, "failed to send email"

    return from_orguser(orguser), None


def update_orguser(orguser: OrgUser, payload: OrgUserUpdate):
    """updates attributes of an OrgUser"""
    if payload.email:
        orguser.user.email = payload.email.lower().strip()
    if payload.active is not None:
        orguser.user.is_active = payload.active
    if payload.role:
        orguser.role = payload.role
    orguser.user.save()

    logger.info(f"updated orguser {orguser.user.email}")
    return from_orguser(orguser)


def update_orguser_v1(orguser: OrgUser, payload: OrgUserUpdatev1):
    """updates attributes of an OrgUser"""
    if payload.email:
        orguser.user.email = payload.email.lower().strip()
    if payload.active is not None:
        orguser.user.is_active = payload.active
    if payload.role_uuid:
        orguser.new_role = Role.objects.filter(uuid=payload.role_uuid).first()
    if payload.has_seen_resource_sharing_notice is not None:
        orguser.has_seen_resource_sharing_notice = payload.has_seen_resource_sharing_notice
    orguser.user.save()
    orguser.save()

    logger.info(f"updated orguser {orguser.user.email}")
    return from_orguser(orguser)


def delete_orguser_from_org(
    target_org: Org,
    requestor_orguser: OrgUser,
    payload: DeleteOrgUserPayload,
    is_platform_admin: bool = False,
):
    """
    org-parameterized core of "remove a user from an org".

    The target org is passed explicitly instead of being read from
    requestor_orguser.org, so the admin portal can remove a user from an org the
    requestor does not belong to. When is_platform_admin is True the role-level cap is
    skipped — a platform admin acting cross-org has no role in target_org to compare
    against. See features/admin-portal/plan.md §4.4.

    NOTE: deleting the OrgUser REASSIGNS the content it created to requestor_orguser
    rather than deleting or orphaning it (upstream #c6b3d545). Callers should still
    surface the removal-impact count first (see the admin removal-impact endpoint,
    plan.md §4.6 / research §5).
    """
    orguser_to_delete = OrgUser.objects.filter(org=target_org, user__email=payload.email).first()

    if requestor_orguser == orguser_to_delete:
        return None, "user cannot delete themselves"

    if orguser_to_delete is None:
        return None, "user does not belong to the org"

    if (
        not is_platform_admin
        and orguser_to_delete.new_role.level > requestor_orguser.new_role.level
    ):
        return None, "cannot delete user having higher role"

    # Reassign resources owned by the removed user to the admin doing the removal so
    # nothing is left orphaned. Scoped to target_org, not requestor_orguser.org: a
    # platform admin removing cross-org is not a member of the org being cleaned up.
    with transaction.atomic():
        for Model in (Dashboard, Chart, Metric, KPI, ReportSnapshot, Alert):
            Model.objects.filter(org=target_org, created_by=orguser_to_delete).update(
                created_by=requestor_orguser
            )

        # remove the pending invitations for this email in the target org
        Invitation.objects.filter(invited_in_org=target_org, invited_email=payload.email).delete()

        # delete the org user
        orguser_to_delete.delete()

    return None, None


def delete_orguser_v1(requestor_orguser: OrgUser, payload: DeleteOrgUserPayload):
    """delete another orguser in the requestor's own org (single-org wrapper)"""
    return delete_orguser_from_org(requestor_orguser.org, requestor_orguser, payload)


def invite_user_to_org(
    target_org: Org,
    inviter_orguser: OrgUser,
    payload: NewInvitationSchema,
    is_platform_admin: bool = False,
    group_name: str = None,
):
    """
    org-parameterized core of "invite a user to an org".

    The target org is passed explicitly instead of being read from
    inviter_orguser.org, so the admin portal can invite into an org the inviter does
    not belong to (including an org with zero members). The new Invitation records
    invited_in_org=target_org, which may differ from inviter_orguser.org when a
    platform admin invites cross-org — so accept/cancel resolve the correct org
    regardless of who sent the invite. When is_platform_admin is True the inviter-level
    cap is skipped: a platform admin may invite at any role. See plan.md §4.4.

    ``group_name`` — set when the invite originates from a group create / edit flow, so
    the email copy names the group instead of the plain "invited to Dalgo" / "added to
    org" wording.
    """
    frontend_url = os.getenv("FRONTEND_URL")

    if target_org is None:
        return None, "create an organization first"

    invited_email = payload.invited_email.lower().strip()
    if OrgUser.objects.filter(org=target_org, user__email__iexact=invited_email).exists():
        return None, "user already has an account"

    invited_role = Role.objects.filter(uuid=payload.invited_role_uuid).first()
    if not invited_role:
        return None, "Invalid role"

    # a regular inviter can only invite at their own level or lower; a platform admin
    # acting cross-org has no role in target_org, so the cap is skipped for them.
    if not is_platform_admin and invited_role.level > inviter_orguser.new_role.level:
        return None, "Insufficient permissions for this operation"

    existing_user = User.objects.filter(email__iexact=invited_email).first()

    if existing_user:
        logger.info("user exists, creating new OrgUser")
        OrgUser.objects.create(user=existing_user, org=target_org, new_role=invited_role)
        user_notifications.send_added_to_org(
            invited_email, inviter_orguser.user.email, target_org.name, group_name=group_name
        )
        return (
            NewInvitationSchema(
                invited_email=invited_email,
                invited_role_uuid=payload.invited_role_uuid,
            ),
            None,
        )

    invitation = Invitation.objects.filter(
        invited_email__iexact=invited_email, invited_in_org=target_org
    ).first()
    if invitation:
        invitation.invited_on = timezone.as_utc(datetime.utcnow())
        # if the invitation is already present - trigger the email again
        invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
        user_notifications.send_invite_user(
            invitation.invited_email,
            invitation.invited_by.user.email,
            invite_url,
            org_name=target_org.name,
            group_name=group_name,
        )
        logger.info(
            f"Resent invitation to {invited_email} to join {target_org.name} "
            f"with invite code {invitation.invite_code}",
        )
        return from_invitation(invitation), None

    invitation = Invitation.objects.create(
        invited_email=invited_email,
        invited_by=inviter_orguser,
        invited_in_org=target_org,
        invited_on=datetime.now(timezone.UTC),
        invite_code=str(uuid4()),
        invited_new_role=invited_role,
    )

    # trigger an email to the user
    invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
    user_notifications.send_invite_user(
        invitation.invited_email,
        invitation.invited_by.user.email,
        invite_url,
        org_name=target_org.name,
        group_name=group_name,
    )

    logger.info(
        f"Invited {invited_email} to join {target_org.name} "
        f"with invite code {invitation.invite_code}",
    )
    return payload, None


def invite_user_v1(orguser: OrgUser, payload: NewInvitationSchema, group_name: str = None):
    """invite a user to the caller's own org (single-org wrapper)"""
    return invite_user_to_org(orguser.org, orguser, payload, group_name=group_name)


def change_orguser_role_in_org(
    target_org: Org,
    requestor_orguser: OrgUser,
    toupdate_email: str,
    role_uuid,
    is_platform_admin: bool = False,
):
    """
    org-parameterized core of the role-change logic in post_modify_orguser_role.

    Assigns a role to a user in target_org. The target org is passed explicitly so the
    admin portal can change a role in an org the requestor does not belong to. When
    is_platform_admin is True the "can't assign a role higher than your own" cap is
    skipped. Returns (the updated OrgUser, error) — the caller maps error to an HTTP
    status, and owns its own response shape and audit log. See plan.md §4.4.

    Validation order mirrors the original endpoint for the single-org path: for a
    regular requestor the missing-role check comes first, then role lookup, then the
    level cap.
    """
    if not is_platform_admin:
        if not requestor_orguser.new_role:
            return None, "Insufficient permissions"

    role_to_be_assigned = Role.objects.filter(uuid=role_uuid).first()
    if not role_to_be_assigned:
        return None, "Invalid role"

    if not is_platform_admin and role_to_be_assigned.level > requestor_orguser.new_role.level:
        return None, "Insufficient permissions"

    request_email = toupdate_email.lower().strip()
    query = OrgUser.objects.filter(user__email__iexact=request_email, org=target_org)
    # a requestor who is a member of target_org may not change their own role; a
    # platform admin acting cross-org is not a member, so nothing to exclude.
    if requestor_orguser is not None and requestor_orguser.org_id == target_org.id:
        query = query.exclude(user__email__iexact=requestor_orguser.user.email)
    orguser_to_be_assigned = query.first()

    if not orguser_to_be_assigned:
        return None, "User does not exist"

    orguser_to_be_assigned.new_role = role_to_be_assigned
    orguser_to_be_assigned.save()

    return orguser_to_be_assigned, None


def accept_invitation_v1(payload: AcceptInvitationSchema):
    """accept an invitation"""
    invitation = Invitation.objects.filter(invite_code=payload.invite_code).first()
    if invitation is None:
        return None, "invalid invite code"

    # the org this invite grants membership of. Prefer the explicit invited_in_org
    # (set on every new invite, and backfilled onto every pre-migration row); fall
    # back to invited_by.org when it is null, so an existing pending invitation still
    # resolves to exactly the same org it did before invited_in_org existed. For a
    # cross-org admin invite invited_in_org is the target org, NOT invited_by.org
    # (the platform admin's own org). See plan.md §4.4.
    target_org = invitation.invited_in_org or invitation.invited_by.org

    # we can have one auth user mapped to multiple orguser and hence multiple orgs
    # but there can only be one orguser per one org
    orguser = OrgUser.objects.filter(
        user__email__iexact=invitation.invited_email, org=target_org
    ).first()

    if not orguser:
        user = User.objects.filter(
            username=invitation.invited_email,
            email=invitation.invited_email,
        ).first()
        if user is None:
            if payload.password is None:
                return None, "password is required"
            logger.info(f"creating invited user {invitation.invited_email} for {target_org.name}")
            user = User.objects.create_user(
                username=invitation.invited_email.lower().strip(),
                email=invitation.invited_email.lower().strip(),
                password=payload.password,
            )
            UserAttributes.objects.create(user=user, email_verified=True)
        orguser = OrgUser.objects.create(
            user=user,
            org=target_org,
            new_role=invitation.invited_new_role,
            work_domain=payload.work_domain,
        )

    # Preserve any group memberships that were pinned to this invitation:
    # promote each member row to point at the accepting orguser instead. If
    # the orguser is already a direct member of that group, drop the
    # invitation-linked row to avoid duplicates. After this, invitation.delete()
    # nulls out any remaining invitation_id via SET_NULL.
    from ddpui.models.org_user import OrgUserGroupMember  # local import to avoid cycles
    from ddpui.models.resource_share import ResourceShare, ResourceSharePrincipalType, ResourceType

    invitation_member_rows = OrgUserGroupMember.objects.filter(invitation=invitation)
    existing_group_ids = set(
        OrgUserGroupMember.objects.filter(orguser=orguser).values_list("group_id", flat=True)
    )
    for member_row in invitation_member_rows:
        if member_row.group_id in existing_group_ids:
            member_row.delete()
        else:
            member_row.orguser = orguser
            member_row.save(update_fields=["orguser", "updated_at"])
            existing_group_ids.add(member_row.group_id)

    # Promote any pending resource shares in the same way: point them at the
    # accepting orguser as a direct user grant. If the orguser already has a
    # direct share on the same resource, drop the invitation-linked row.
    invitation_share_rows = ResourceShare.objects.filter(invitation=invitation)
    existing_direct_keys = set(
        ResourceShare.objects.filter(
            org=invitation.invited_by.org,
            principal_type=ResourceSharePrincipalType.USER,
            principal_id=orguser.id,
        ).values_list("resource_type", "resource_id")
    )
    promoted_dashboard_ids: set[int] = set()
    for share_row in invitation_share_rows:
        key = (share_row.resource_type, share_row.resource_id)
        if key in existing_direct_keys:
            share_row.delete()
        else:
            share_row.principal_type = ResourceSharePrincipalType.USER
            share_row.principal_id = orguser.id
            share_row.save(update_fields=["principal_type", "principal_id"])
            existing_direct_keys.add(key)
            if share_row.resource_type == ResourceType.DASHBOARD:
                promoted_dashboard_ids.add(int(share_row.resource_id))

    invitation.delete()

    # Cascade the promoted dashboard shares to inner charts/KPIs now that the
    # shares are real user grants (sync_dashboard_cascade skips invitation-backed rows).
    if promoted_dashboard_ids:
        from ddpui.core.access.resource_share import sync_dashboard_cascade

        for dashboard_id in promoted_dashboard_ids:
            dashboard = Dashboard.objects.filter(id=dashboard_id).first()
            if dashboard:
                sync_dashboard_cascade(dashboard)

    return from_orguser(orguser), None


def get_invitations_from_orguser(orguser: OrgUser):
    """get all invitations sent by an orguser"""
    if orguser.org is None:
        return None, "create an organization first"

    invitations = Invitation.objects.filter(invited_by=orguser).order_by("-invited_on").all()
    res = []
    for invitation in invitations:
        res.append(
            {
                "id": invitation.id,
                "invited_email": invitation.invited_email,
                "invited_role_slug": slugify(OrgUserRole(invitation.invited_role).name),
                "invited_role": invitation.invited_role,
                "invited_on": invitation.invited_on,
            }
        )

    return res, None


def get_invitations_from_orguser_v1(orguser: OrgUser):
    """get all invitations sent by an orguser"""
    if orguser.org is None:
        return None, "create an organization first"

    invitations = Invitation.objects.filter(invited_by=orguser).order_by("-invited_on").all()
    res = []
    for invitation in invitations:
        res.append(
            {
                "id": invitation.id,
                "invited_email": invitation.invited_email,
                "invited_role": {
                    "uuid": invitation.invited_new_role.uuid,
                    "name": invitation.invited_new_role.name,
                },
                "invited_on": invitation.invited_on,
            }
        )

    return res, None


def resend_invitation(invitation_id: str):
    """resend email invitation to user"""
    invitation = Invitation.objects.filter(id=invitation_id).first()

    if invitation is None:
        return None, "invitation not found"

    invitation.invited_on = timezone.as_utc(datetime.utcnow())
    invitation.save()
    # trigger an email to the user
    frontend_url = os.getenv("FRONTEND_URL")
    invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
    user_notifications.send_invite_user(
        invitation.invited_email,
        invitation.invited_by.user.email,
        invite_url,
        org_name=invitation.invited_by.org.name,
    )

    return None, None


def request_reset_password(email: str, is_v2: bool = False):
    """send the reset password email"""
    orguser = OrgUser.objects.filter(user__email=email, user__is_active=True).first()

    if orguser is None:
        # we don't leak any information about which email
        # addresses exist in our database
        return None, None

    redis = RedisClient.get_instance()
    token = uuid4()

    redis_key = f"password-reset:{token.hex}"
    orguserid_bytes = str(orguser.id).encode("utf8")

    redis.set(redis_key, orguserid_bytes)
    redis.expire(redis_key, 3600 * 24)  # 24 hours

    # To seperate the frontend urls for v1 and v2
    FRONTEND_URL = os.getenv("FRONTEND_URL_V2") if is_v2 else os.getenv("FRONTEND_URL")

    reset_url = f"{FRONTEND_URL}/resetpassword?token={token.hex}"

    try:
        user_notifications.send_password_reset(email, reset_url)
    except Exception:
        return None, "failed to send email"

    return None, None


def confirm_reset_password(payload: ResetPasswordSchema):
    """verify the reset password token and reset the password"""
    redis = RedisClient.get_instance()
    redis_key = f"password-reset:{payload.token}"
    password_reset = redis.get(redis_key)
    if password_reset is None:
        return None, "invalid reset code"

    redis.delete(redis_key)
    orguserid_str = password_reset.decode("utf8")
    orguser = OrgUser.objects.filter(id=int(orguserid_str)).first()
    if orguser is None:
        logger.error("no orguser having id %s", orguserid_str)
        return None, "could not look up request from this token"

    orguser.user.set_password(payload.password.get_secret_value())
    orguser.user.save()

    return orguser, None


def change_password(payload: ChangePasswordSchema, orguser: OrgUser):
    """If password and confirm password are same reset the password"""

    if payload.password != payload.confirmPassword:
        return None, "Password and confirm password must be same"

    orguser.user.set_password(payload.password.get_secret_value())
    orguser.user.save()

    return None, None


def resend_verification_email(orguser: OrgUser, email: str):
    """send a verification email to the user"""
    redis = RedisClient.get_instance()
    token = uuid4()

    redis_key = f"email-verification:{token.hex}"
    orguserid_bytes = str(orguser.id).encode("utf8")

    redis.set(redis_key, orguserid_bytes)

    FRONTEND_URL = os.getenv("FRONTEND_URL")
    reset_url = f"{FRONTEND_URL}/verifyemail/?token={token.hex}"
    try:
        user_notifications.send_signup(email, reset_url)
    except Exception:
        return None, "failed to send email"

    return None, None


def verify_email(payload: VerifyEmailSchema):
    """verify the email verification token"""
    redis = RedisClient.get_instance()
    redis_key = f"email-verification:{payload.token}"
    verify_email_token = redis.get(redis_key)
    if verify_email_token is None:
        return None, "this link has expired"

    redis.delete(redis_key)
    orguserid_str = verify_email_token.decode("utf8")
    orguser = OrgUser.objects.filter(id=int(orguserid_str)).first()
    if orguser is None:
        logger.error("no orguser having id %s", orguserid_str)
        return None, "could not look up request from this token"

    # verify email for all the orgusers
    OrgUser.objects.filter(user_id=orguser.user.id).update(
        email_verified=True, updated_at=django_timezone.now()
    )
    UserAttributes.objects.filter(user=orguser.user).update(
        email_verified=True, updated_at=django_timezone.now()
    )

    return orguser, None


def ensure_orguser_for_org(orguser: OrgUser, org):
    """
    adds the org to the orguser if there isn't one already
    otherwise create a new orguser for this org
    """
    if orguser.org is None:
        orguser.org = org
        orguser.save()
    else:
        OrgUser.objects.create(
            user=orguser.user,
            email_verified=True,
            org=org,
            new_role=orguser.new_role,
        )
    return None, None


def accept_tnc(orguser: OrgUser):
    """accept the terms and conditions"""
    if orguser.org is None:
        return None, "create an organization first"

    userattributes = UserAttributes.objects.filter(user=orguser.user).first()
    if userattributes and userattributes.is_consultant:
        return None, "user cannot accept tnc"

    if OrgTnC.objects.filter(org=orguser.org).exists():
        return None, "tnc already accepted"

    OrgTnC.objects.create(org=orguser.org, tnc_accepted_by=orguser, tnc_accepted_on=datetime.now())

    return None, None
