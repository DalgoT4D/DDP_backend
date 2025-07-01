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

from ddpui.auth import ACCOUNT_MANAGER_ROLE, GUEST_ROLE
from ddpui.models.org import Org, OrgType
from ddpui.models.org_user import (
    AcceptInvitationSchema,
    DeleteOrgUserPayload,
    Invitation,
    InvitationSchema,
    NewInvitationSchema,
    OrgUser,
    OrgUserCreate,
    OrgUserUpdate,
    OrgUserUpdatev1,
    ResetPasswordSchema,
    ChangePasswordSchema,
    UserAttributes,
    VerifyEmailSchema,
)
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.orgtnc import OrgTnC
from ddpui.models.role_based_access import Role
from ddpui.utils import helpers, awsses, timezone
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.orguserhelpers import from_invitation, from_orguser
from ddpui.utils.redis_client import RedisClient

logger = CustomLogger("ddpui")


def lookup_user(email: str):
    """look up user by username"""
    user = User.objects.filter(email=email).first()

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
        "is_platform_admin": userattributes.is_platform_admin,
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
    orguser = OrgUser.objects.create(
        user=user,
        org=demo_org,
        new_role=(
            Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()
            if is_demo
            else Role.objects.filter(slug=GUEST_ROLE).first()
        ),
        email_verified=email_verified,
    )
    orguser.save()
    UserPreferences.objects.create(orguser=orguser, enable_email_notifications=True)
    logger.info(
        f"created user [account-manager] " f"{orguser.user.email} having userid {orguser.user.id}"
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
        awsses.send_signup_email(payload.email, reset_url)
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
    orguser.user.save()
    orguser.save()

    logger.info(f"updated orguser {orguser.user.email}")
    return from_orguser(orguser)


def delete_orguser(requestor_orguser: OrgUser, payload: DeleteOrgUserPayload):
    """delete another orguser"""
    orguser_to_delete = OrgUser.objects.filter(
        org=requestor_orguser.org, user__email=payload.email
    ).first()

    if requestor_orguser == orguser_to_delete:
        return None, "user cannot delete themselves"

    if orguser_to_delete is None:
        return None, "user does not belong to the org"

    if orguser_to_delete.role > requestor_orguser.role:
        return None, "cannot delete user having higher role"

    # remove the invitations associated with the org user
    Invitation.objects.filter(
        invited_by__org=requestor_orguser.org, invited_email=payload.email
    ).delete()

    # delete the org user
    orguser_to_delete.delete()

    return None, None


def delete_orguser_v1(requestor_orguser: OrgUser, payload: DeleteOrgUserPayload):
    """delete another orguser"""
    orguser_to_delete = OrgUser.objects.filter(
        org=requestor_orguser.org, user__email=payload.email
    ).first()

    if requestor_orguser == orguser_to_delete:
        return None, "user cannot delete themselves"

    if orguser_to_delete is None:
        return None, "user does not belong to the org"

    if orguser_to_delete.new_role.level > requestor_orguser.new_role.level:
        return None, "cannot delete user having higher role"

    # remove the invitations associated with the org user
    Invitation.objects.filter(
        invited_by__org=requestor_orguser.org, invited_email=payload.email
    ).delete()

    # delete the org user
    orguser_to_delete.delete()

    return None, None


def invite_user(orguser: OrgUser, payload: InvitationSchema):
    """invite a user to an org"""
    frontend_url = os.getenv("FRONTEND_URL")

    logger.info(payload)

    if orguser.org is None:
        return None, "create an organization first"

    invited_email = payload.invited_email.lower().strip()
    if OrgUser.objects.filter(org=orguser.org, user__email__iexact=invited_email).exists():
        return None, "user already has an account"

    role_slugs = OrgUserRole.role_slugs()
    if payload.invited_role_slug not in role_slugs:
        return None, "Invalid role"

    invited_role = role_slugs[payload.invited_role_slug]

    # user can only invite a role equal or lower to their role
    if invited_role > orguser.role:
        return None, "Insufficient permissions for this operation"

    existing_user = User.objects.filter(email__iexact=invited_email).first()

    # new role for the invited user; so that he doesnt' get stuck due new RBAC changes released
    new_role = Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first()

    if existing_user:
        logger.info("user exists, creating new OrgUser")
        OrgUser.objects.create(
            user=existing_user, org=orguser.org, role=invited_role, new_role=new_role
        )
        awsses.send_youve_been_added_email(invited_email, orguser.user.email, orguser.org.name)
        return (
            InvitationSchema(
                invited_email=invited_email,
                invited_role_slug=payload.invited_role_slug,
            ),
            None,
        )

    invitation = Invitation.objects.filter(
        invited_email__iexact=invited_email, invited_by__org=orguser.org
    ).first()
    if invitation:
        invitation.invited_on = timezone.as_utc(datetime.utcnow())
        # if the invitation is already present - trigger the email again
        invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
        awsses.send_invite_user_email(
            invitation.invited_email, invitation.invited_by.user.email, invite_url
        )
        logger.info(
            f"Resent invitation to {invited_email} to join {orguser.org.name} "
            f"with invite code {invitation.invite_code}",
        )
        return from_invitation(invitation), None

    payload.invited_by = from_orguser(orguser)
    payload.invited_on = timezone.as_utc(datetime.utcnow())
    payload.invite_code = str(uuid4())

    invitation = Invitation.objects.create(
        invited_email=invited_email,
        invited_role=invited_role,
        invited_by=orguser,
        invited_on=payload.invited_on,
        invite_code=payload.invite_code,
        invited_new_role=new_role,
    )

    # trigger an email to the user
    invite_url = f"{frontend_url}/invitations/?invite_code={payload.invite_code}"
    awsses.send_invite_user_email(
        invitation.invited_email, invitation.invited_by.user.email, invite_url
    )

    logger.info(
        f"Invited {invited_email} to join {orguser.org.name} "
        f"with invite code {payload.invite_code}",
    )
    return payload, None


def invite_user_v1(orguser: OrgUser, payload: NewInvitationSchema):
    """invite a user to an org"""
    frontend_url = os.getenv("FRONTEND_URL")

    if orguser.org is None:
        return None, "create an organization first"

    invited_email = payload.invited_email.lower().strip()
    if OrgUser.objects.filter(org=orguser.org, user__email__iexact=invited_email).exists():
        return None, "user already has an account"

    invited_role = Role.objects.filter(uuid=payload.invited_role_uuid).first()
    if not invited_role:
        return None, "Invalid role"

    # user can only invite a role equal or lower to their role
    if invited_role.level > orguser.new_role.level:
        return None, "Insufficient permissions for this operation"

    existing_user = User.objects.filter(email__iexact=invited_email).first()

    if existing_user:
        logger.info("user exists, creating new OrgUser")
        OrgUser.objects.create(user=existing_user, org=orguser.org, new_role=invited_role)
        awsses.send_youve_been_added_email(invited_email, orguser.user.email, orguser.org.name)
        return (
            NewInvitationSchema(
                invited_email=invited_email,
                invited_role_uuid=payload.invited_role_uuid,
            ),
            None,
        )

    invitation = Invitation.objects.filter(
        invited_email__iexact=invited_email, invited_by__org=orguser.org
    ).first()
    if invitation:
        invitation.invited_on = timezone.as_utc(datetime.utcnow())
        # if the invitation is already present - trigger the email again
        invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
        awsses.send_invite_user_email(
            invitation.invited_email, invitation.invited_by.user.email, invite_url
        )
        logger.info(
            f"Resent invitation to {invited_email} to join {orguser.org.name} "
            f"with invite code {invitation.invite_code}",
        )
        return from_invitation(invitation), None

    # payload.invited_by = from_orguser(orguser)
    # payload.invited_on = timezone.as_utc(datetime.utcnow())
    # payload.invite_code = str(uuid4())

    invitation = Invitation.objects.create(
        invited_email=invited_email,
        invited_by=orguser,
        invited_on=datetime.now(timezone.UTC),
        invite_code=str(uuid4()),
        invited_new_role=invited_role,
    )

    # trigger an email to the user
    invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
    awsses.send_invite_user_email(
        invitation.invited_email, invitation.invited_by.user.email, invite_url
    )

    logger.info(
        f"Invited {invited_email} to join {orguser.org.name} "
        f"with invite code {invitation.invite_code}",
    )
    return payload, None


def accept_invitation_v1(payload: AcceptInvitationSchema):
    """accept an invitation"""
    invitation = Invitation.objects.filter(invite_code=payload.invite_code).first()
    if invitation is None:
        return None, "invalid invite code"

    # we can have one auth user mapped to multiple orguser and hence multiple orgs
    # but there can only be one orguser per one org
    orguser = OrgUser.objects.filter(
        user__email__iexact=invitation.invited_email, org=invitation.invited_by.org
    ).first()

    if not orguser:
        user = User.objects.filter(
            username=invitation.invited_email,
            email=invitation.invited_email,
        ).first()
        if user is None:
            if payload.password is None:
                return None, "password is required"
            logger.info(
                f"creating invited user {invitation.invited_email} "
                f"for {invitation.invited_by.org.name}"
            )
            user = User.objects.create_user(
                username=invitation.invited_email.lower().strip(),
                email=invitation.invited_email.lower().strip(),
                password=payload.password,
            )
            UserAttributes.objects.create(user=user, email_verified=True)
        orguser = OrgUser.objects.create(
            user=user,
            org=invitation.invited_by.org,
            new_role=invitation.invited_new_role,
        )
    invitation.delete()
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

    if invitation:
        invitation.invited_on = timezone.as_utc(datetime.utcnow())
        invitation.save()
        # trigger an email to the user
        frontend_url = os.getenv("FRONTEND_URL")
        invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
        awsses.send_invite_user_email(
            invitation.invited_email, invitation.invited_by.user.email, invite_url
        )

    return None, None


def request_reset_password(email: str):
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

    FRONTEND_URL = os.getenv("FRONTEND_URL")
    reset_url = f"{FRONTEND_URL}/resetpassword/?token={token.hex}"
    try:
        awsses.send_password_reset_email(email, reset_url)
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

    return None, None


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
        awsses.send_signup_email(email, reset_url)
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

    return None, None


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
            role=OrgUserRole.ACCOUNT_MANAGER,
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
