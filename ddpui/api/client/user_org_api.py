from datetime import datetime
from typing import List
from uuid import uuid4
import json
import os
from dotenv import load_dotenv
from redis import Redis

from django.contrib.auth.models import User
from django.utils.text import slugify
from django.db import transaction
from ninja import NinjaAPI
from ninja.errors import HttpError

from ninja.errors import ValidationError
from ninja.responses import Response
from pydantic.error_wrappers import ValidationError as PydanticValidationError
from rest_framework.authtoken import views

from ddpui import auth
from ddpui.models.org import (
    Org,
    OrgSchema,
    OrgWarehouse,
    OrgWarehouseSchema,
    OrgPrefectBlock,
    OrgDataFlow,
)
from ddpui.models.org_user import (
    AcceptInvitationSchema,
    Invitation,
    InvitationSchema,
    OrgUser,
    OrgUserCreate,
    OrgUserNewOwner,
    OrgUserResponse,
    OrgUserRole,
    OrgUserUpdate,
    ForgotPasswordSchema,
    ResetPasswordSchema,
    VerifyEmailSchema,
    DeleteOrgUserPayload,
)
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service, airbytehelpers
from ddpui.ddpdbt import dbt_service
from ddpui.ddpprefect import AIRBYTECONNECTION
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils import sendgrid
from ddpui.utils import helpers
from ddpui.utils import timezone

user_org_api = NinjaAPI(urls_namespace="userorg")
# http://127.0.0.1:8000/api/docs

load_dotenv()

logger = CustomLogger("ddpui")


@user_org_api.exception_handler(ValidationError)
def ninja_validation_error_handler(request, exc):  # pylint: disable=unused-argument
    """
    Handle any ninja validation errors raised in the apis
    These are raised during request payload validation
    exc.errors is correct
    """
    return Response({"detail": exc.errors}, status=422)


@user_org_api.exception_handler(PydanticValidationError)
def pydantic_validation_error_handler(
    request, exc: PydanticValidationError
):  # pylint: disable=unused-argument
    """
    Handle any pydantic errors raised in the apis
    These are raised during response payload validation
    exc.errors() is correct
    """
    return Response({"detail": exc.errors()}, status=500)


@user_org_api.exception_handler(Exception)
def ninja_default_error_handler(
    request, exc: Exception
):  # pylint: disable=unused-argument # skipcq PYL-W0613
    """Handle any other exception raised in the apis"""
    print(exc)
    return Response({"detail": "something went wrong"}, status=500)


@user_org_api.get("/currentuser", response=OrgUserResponse, auth=auth.AnyOrgUser())
def get_current_user(request):
    """return the OrgUser making this request"""
    orguser: OrgUser = request.orguser
    if orguser is not None:
        return OrgUserResponse.from_orguser(orguser)
    raise HttpError(400, "requestor is not an OrgUser")


@user_org_api.get(
    "/currentuserv2", response=List[OrgUserResponse], auth=auth.AnyOrgUser()
)
def get_current_user_v2(request):
    """return all the OrgUsers for the User making this request"""
    if request.orguser is None:
        raise HttpError(400, "requestor is not an OrgUser")
    user = request.orguser.user
    return [
        OrgUserResponse.from_orguser(orguser)
        for orguser in OrgUser.objects.filter(user=user)
    ]


@user_org_api.post("/organizations/users/", response=OrgUserResponse)
def post_organization_user(
    request, payload: OrgUserCreate
):  # pylint: disable=unused-argument
    """this is the "signup" action
    creates a new OrgUser having specified email + password.
    no Org is created or attached at this time
    """
    signupcode = payload.signupcode
    if signupcode != os.getenv("SIGNUPCODE"):
        raise HttpError(400, "That is not the right signup code")
    email = payload.email.lower().strip()
    if User.objects.filter(email=email).exists():
        raise HttpError(400, f"user having email {email} exists")
    if User.objects.filter(username=email).exists():
        raise HttpError(400, f"user having email {email} exists")
    if not helpers.isvalid_email(email):
        raise HttpError(400, "that is not a valid email address")

    user = User.objects.create_user(
        username=email, email=email, password=payload.password
    )
    orguser = OrgUser.objects.create(user=user, role=OrgUserRole.ACCOUNT_MANAGER)
    orguser.save()
    logger.info(
        f"created user [account-manager] "
        f"{orguser.user.email} having userid {orguser.user.id}"
    )
    redis = Redis()
    token = uuid4()

    redis_key = f"email-verification:{token.hex}"
    orguserid_bytes = str(orguser.id).encode("utf8")

    redis.set(redis_key, orguserid_bytes)

    FRONTEND_URL = os.getenv("FRONTEND_URL")
    reset_url = f"{FRONTEND_URL}/verifyemail/?token={token.hex}"
    try:
        sendgrid.send_signup_email(payload.email, reset_url)
    except Exception as error:
        raise HttpError(400, "failed to send email") from error
    return OrgUserResponse.from_orguser(orguser)


@user_org_api.post("/login/")
def post_login(request):
    """Uses the username and password in the request to return an auth token"""
    request_obj = json.loads(request.body)
    token = views.obtain_auth_token(request)
    if "token" in token.data:
        user = User.objects.filter(email=request_obj["username"]).first()

        # check if all the orgusers for this user have email verified
        email_verified = OrgUser.objects.filter(user=user, email_verified=True).exists()
        if email_verified:
            OrgUser.objects.filter(user=user, email_verified=False).update(
                email_verified=True
            )

        return {
            "token": token.data["token"],
            "email": user.email,
            "email_verified": email_verified,
            "active": user.is_active,
        }

    return token


@user_org_api.get(
    "/organizations/users", response=List[OrgUserResponse], auth=auth.CanManageUsers()
)
def get_organization_users(request):
    """list all OrgUsers in the requestor's org, including inactive"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    query = OrgUser.objects.filter(org=orguser.org)
    return [OrgUserResponse.from_orguser(orguser) for orguser in query]


@user_org_api.post("/organizations/users/delete", auth=auth.CanManageUsers())
def delete_organization_users(request, payload: DeleteOrgUserPayload):
    """delete the orguser posted"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")

    orguser_delete = OrgUser.objects.filter(
        org=orguser.org, user__email=payload.email
    ).first()

    if orguser == orguser_delete:
        raise HttpError(400, "user cannot delete themselves")

    if orguser_delete is None:
        raise HttpError(400, "user does not belong to the org")

    if orguser_delete.role > orguser.role:
        raise HttpError(400, "cannot delete user having higher role")

    # remove the invitations associated with the org user
    Invitation.objects.filter(
        invited_by__org=orguser.org, invited_email=payload.email
    ).delete()

    # delete the org user
    orguser_delete.delete()

    return {"success": 1}


@user_org_api.put(
    "/organizations/user_self/", response=OrgUserResponse, auth=auth.AnyOrgUser()
)
def put_organization_user_self(request, payload: OrgUserUpdate):
    """update the requestor's OrgUser"""
    orguser: OrgUser = request.orguser

    if payload.email:
        orguser.user.email = payload.email.lower().strip()
    if payload.active is not None:
        orguser.user.is_active = payload.active
    orguser.user.save()

    logger.info(f"updated self {orguser.user.email}")
    return OrgUserResponse.from_orguser(orguser)


@user_org_api.put(
    "/organizations/users/",
    response=OrgUserResponse,
    auth=auth.CanManageUsers(),
)
def put_organization_user(request, payload: OrgUserUpdate):
    """update another OrgUser"""
    requestor_orguser: OrgUser = request.orguser

    if requestor_orguser.role not in [
        OrgUserRole.ACCOUNT_MANAGER,
        OrgUserRole.PIPELINE_MANAGER,
    ]:
        raise HttpError(400, "not authorized to update another user")

    orguser = OrgUser.objects.filter(
        user__email=payload.toupdate_email, org=request.orguser.org
    ).first()

    if payload.email:
        orguser.user.email = payload.email.lower().strip()
    if payload.active is not None:
        orguser.user.is_active = payload.active
    if payload.role:
        orguser.role = payload.role
    orguser.user.save()

    logger.info(f"updated orguser {orguser.user.email}")
    return OrgUserResponse.from_orguser(orguser)


@user_org_api.post(
    "/organizations/users/makeowner/",
    response=OrgUserResponse,
    auth=auth.FullAccess(),
)
def post_transfer_ownership(request, payload: OrgUserNewOwner):
    """update another OrgUser"""
    requestor_orguser: OrgUser = request.orguser

    if requestor_orguser.role not in [
        OrgUserRole.ACCOUNT_MANAGER,
    ]:
        raise HttpError(400, "only an account owner can transfer account ownership")

    new_owner = OrgUser.objects.filter(
        org=requestor_orguser.org,
        user__email=payload.new_owner_email,
        user__is_active=True,
    ).first()

    if new_owner is None:
        raise HttpError(
            400, "could not find user having this email address in this org"
        )

    if new_owner.role not in [OrgUserRole.PIPELINE_MANAGER]:
        raise HttpError(400, "can only promote pipeline managers")

    new_owner.role = OrgUserRole.ACCOUNT_MANAGER
    requestor_orguser.role = OrgUserRole.PIPELINE_MANAGER
    try:
        with transaction.atomic():
            new_owner.save()
            requestor_orguser.save()
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to transfer ownership") from error

    return OrgUserResponse.from_orguser(requestor_orguser)


@user_org_api.post("/organizations/", response=OrgSchema, auth=auth.AnyOrgUser())
def post_organization(request, payload: OrgSchema):
    """creates a new org & new orguser (if required) and attaches it to the requestor"""
    orguser: OrgUser = request.orguser
    org = Org.objects.filter(name__iexact=payload.name).first()
    if org:
        raise HttpError(400, "client org with this name already exists")

    org = Org(name=payload.name)
    org.slug = slugify(org.name)[:20]
    org.save()
    logger.info(f"{orguser.user.email} created new org {org.name}")
    try:
        new_workspace = airbytehelpers.setup_airbyte_workspace(org.slug, org)
    except Exception as error:
        # delete the org or we won't be able to create it once airbyte comes back up
        org.delete()
        raise HttpError(400, "could not create airbyte workspace") from error

    # create a new orguser if the org is already there
    if orguser.org is None:
        orguser.org = org
        orguser.save()
    else:
        orguser = OrgUser.objects.create(
            user=orguser.user,
            role=OrgUserRole.ACCOUNT_MANAGER,
            email_verified=True,
            org=org,
        )

    return OrgSchema(name=org.name, airbyte_workspace_id=new_workspace.workspaceId)


@user_org_api.post("/organizations/warehouse/", auth=auth.CanManagePipelines())
def post_organization_warehouse(request, payload: OrgWarehouseSchema):
    """registers a data warehouse for the org"""
    orguser: OrgUser = request.orguser
    if payload.wtype not in ["postgres", "bigquery"]:
        raise HttpError(400, "unrecognized warehouse type " + payload.wtype)

    destination = airbyte_service.create_destination(
        orguser.org.airbyte_workspace_id,
        f"{payload.wtype}-warehouse",
        payload.destinationDefId,
        payload.airbyteConfig,
    )
    logger.info("created destination having id " + destination["destinationId"])

    # prepare the dbt credentials from airbyteConfig
    dbt_credentials = None
    if payload.wtype == "postgres":
        dbt_credentials = {
            "host": payload.airbyteConfig["host"],
            "port": payload.airbyteConfig["port"],
            "username": payload.airbyteConfig["username"],
            "password": payload.airbyteConfig["password"],
            "database": payload.airbyteConfig["database"],
        }

    elif payload.wtype == "bigquery":
        dbt_credentials = json.loads(payload.airbyteConfig["credentials_json"])

    warehouse = OrgWarehouse(
        org=orguser.org,
        wtype=payload.wtype,
        credentials="",
        airbyte_destination_id=destination["destinationId"],
    )
    credentials_lookupkey = secretsmanager.save_warehouse_credentials(
        warehouse, dbt_credentials
    )
    warehouse.credentials = credentials_lookupkey
    warehouse.save()
    return {"success": 1}


@user_org_api.delete("/organizations/warehouses/", auth=auth.CanManagePipelines())
def delete_organization_warehouses(request):
    """deletes all (references to) data warehouses for the org"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    warehouse = OrgWarehouse.objects.filter(org=orguser.org).first()
    if warehouse is None:
        raise HttpError(400, "warehouse not created")

    # delete prefect connection blocks
    logger.info("Deleting prefect connection blocks")
    for block in OrgPrefectBlock.objects.filter(
        org=orguser.org, block_type=AIRBYTECONNECTION
    ):
        try:
            prefect_service.delete_airbyte_connection_block(block.block_id)
            logger.info(f"delete connecion block id - {block.block_id}")
        except Exception:  # skipcq PYL-W0703
            logger.error(
                "failed to delete %s airbyte-connection-block %s in prefect, deleting from OrgPrefectBlock",
                orguser.org.slug,
                block.block_id,
            )
        block.delete()

    logger.info("FINISHED Deleting prefect connection blocks")

    # delete airbyte connections
    logger.info("Deleting airbyte connections")
    for connection in airbyte_service.get_connections(orguser.org.airbyte_workspace_id)[
        "connections"
    ]:
        connection_id = connection["connectionId"]
        airbyte_service.delete_connection(
            orguser.org.airbyte_workspace_id, connection_id
        )
        logger.info(f"deleted connection in Airbyte - {connection_id}")

    logger.info("FINISHED Deleting airbyte connections")

    # delete airbyte destinations
    logger.info("Deleting airbyte destinations")
    for destination in airbyte_service.get_destinations(
        orguser.org.airbyte_workspace_id
    )["destinations"]:
        destination_id = destination["destinationId"]
        airbyte_service.delete_destination(
            orguser.org.airbyte_workspace_id, destination_id
        )
        logger.info(f"deleted destination in Airbyte - {destination_id}")

    logger.info("FINISHED Deleting airbyte destinations")

    # delete django warehouse row
    logger.info("Deleting django warehouse and the credentials in secrets manager")
    secretsmanager.delete_warehouse_credentials(warehouse)
    warehouse.delete()

    # delete dbt workspace and blocks
    dbt_service.delete_dbt_workspace(orguser.org)

    # delete dataflows
    logger.info("Deleting data flows")
    for data_flow in OrgDataFlow.objects.filter(org=orguser.org):
        prefect_service.delete_deployment_by_id(data_flow.deployment_id)
        data_flow.delete()
        logger.info(f"Deleted deployment - {data_flow.deployment_id}")
    logger.info("FINISHED Deleting data flows")

    return {"success": 1}


@user_org_api.get("/organizations/warehouses", auth=auth.CanManagePipelines())
def get_organizations_warehouses(request):
    """returns all warehouses associated with this org"""
    orguser: OrgUser = request.orguser
    warehouses = [
        {
            "wtype": warehouse.wtype,
            # "credentials": warehouse.credentials,
            "airbyte_destination": airbyte_service.get_destination(
                orguser.org.airbyte_workspace_id, warehouse.airbyte_destination_id
            ),
        }
        for warehouse in OrgWarehouse.objects.filter(org=orguser.org)
    ]
    return {"warehouses": warehouses}


@user_org_api.post(
    "/organizations/users/invite/",
    response=InvitationSchema,
    auth=auth.CanManageUsers(),
)
def post_organization_user_invite(request, payload: InvitationSchema):
    """Send an invitation to a user to join platform"""
    orguser: OrgUser = request.orguser
    frontend_url = os.getenv("FRONTEND_URL")

    logger.info(payload)

    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    invited_email = payload.invited_email.lower().strip()
    if OrgUser.objects.filter(
        org=orguser.org, user__email__iexact=invited_email
    ).exists():
        raise HttpError(400, "user already has an account")

    role_slugs = OrgUserRole.role_slugs()
    if payload.invited_role_slug not in role_slugs:
        raise HttpError(404, "Invalid role")

    invited_role = role_slugs[payload.invited_role_slug]

    # user can only invite a role equal or lower to their role
    if invited_role > orguser.role:
        raise HttpError(403, "Insufficient permissions for this operation")

    invitation = Invitation.objects.filter(
        invited_email__iexact=invited_email, invited_by__org=orguser.org
    ).first()
    if invitation:
        invitation.invited_on = timezone.as_utc(datetime.utcnow())
        # if the invitation is already present - trigger the email again
        invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
        sendgrid.send_invite_user_email(
            invitation.invited_email, invitation.invited_by.user.email, invite_url
        )
        logger.info(
            f"Resent invitation to {invited_email} to join {orguser.org.name} "
            f"with invite code {invitation.invite_code}",
        )
        return InvitationSchema.from_invitation(invitation)

    payload.invited_by = OrgUserResponse.from_orguser(orguser)
    payload.invited_on = timezone.as_utc(datetime.utcnow())
    payload.invite_code = str(uuid4())

    invitation = Invitation.objects.create(
        invited_email=invited_email,
        invited_role=invited_role,
        invited_by=orguser,
        invited_on=payload.invited_on,
        invite_code=payload.invite_code,
    )

    # trigger an email to the user
    invite_url = f"{frontend_url}/invitations/?invite_code={payload.invite_code}"
    sendgrid.send_invite_user_email(
        invitation.invited_email, invitation.invited_by.user.email, invite_url
    )

    logger.info(
        f"Invited {invited_email} to join {orguser.org.name} "
        f"with invite code {payload.invite_code}",
    )
    return payload


@user_org_api.post(
    "/organizations/users/invite/accept/",
    response=OrgUserResponse,
)
def post_organization_user_accept_invite(
    request, payload: AcceptInvitationSchema
):  # pylint: disable=unused-argument
    """User accepting the invite sent with a valid invite code"""
    invitation = Invitation.objects.filter(invite_code=payload.invite_code).first()
    if invitation is None:
        raise HttpError(400, "invalid invite code")

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
                raise HttpError(400, "password is required")
            logger.info(
                f"creating invited user {invitation.invited_email} "
                f"for {invitation.invited_by.org.name}"
            )
            user = User.objects.create_user(
                username=invitation.invited_email.lower().strip(),
                email=invitation.invited_email.lower().strip(),
                password=payload.password,
            )
        orguser = OrgUser.objects.create(
            user=user, org=invitation.invited_by.org, role=invitation.invited_role
        )
    invitation.delete()
    return OrgUserResponse.from_orguser(orguser)


@user_org_api.get("/users/invitations/", auth=auth.AnyOrgUser())
def get_invitations(request):
    """Get all invitations sent by the current user"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    invitations = (
        Invitation.objects.filter(invited_by=orguser).order_by("-invited_on").all()
    )
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

    return res


@user_org_api.post("/users/invitations/resend/{invitation_id}", auth=auth.AnyOrgUser())
def post_resend_invitation(request, invitation_id):
    """Get all invitations sent by the current user"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    invitation = Invitation.objects.filter(id=invitation_id).first()

    if invitation:
        invitation.invited_on = timezone.as_utc(datetime.utcnow())
        invitation.save()
        # trigger an email to the user
        frontend_url = os.getenv("FRONTEND_URL")
        invite_url = f"{frontend_url}/invitations/?invite_code={invitation.invite_code}"
        sendgrid.send_invite_user_email(
            invitation.invited_email, invitation.invited_by.user.email, invite_url
        )

    return {"success": 1}


@user_org_api.delete(
    "/users/invitations/delete/{invitation_id}", auth=auth.AnyOrgUser()
)
def delete_invitation(request, invitation_id):
    """Get all invitations sent by the current user"""
    orguser: OrgUser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "create an organization first")

    invitation = Invitation.objects.filter(id=invitation_id).first()

    if invitation:
        invitation.delete()

    return {"success": 1}


@user_org_api.post(
    "/users/forgot_password/",
)
def post_forgot_password(
    request, payload: ForgotPasswordSchema
):  # pylint: disable=unused-argument
    """step 1 of the forgot-password flow"""
    orguser = OrgUser.objects.filter(
        user__email=payload.email, user__is_active=True
    ).first()

    if orguser is None:
        # we don't leak any information about which email
        # addresses exist in our database
        return {"success": 1}

    redis = Redis()
    token = uuid4()

    redis_key = f"password-reset:{token.hex}"
    orguserid_bytes = str(orguser.id).encode("utf8")

    redis.set(redis_key, orguserid_bytes)
    redis.expire(redis_key, 3600 * 24)  # 24 hours

    FRONTEND_URL = os.getenv("FRONTEND_URL")
    reset_url = f"{FRONTEND_URL}/resetpassword/?token={token.hex}"
    try:
        sendgrid.send_password_reset_email(payload.email, reset_url)
    except Exception as error:
        raise HttpError(400, "failed to send email") from error

    return {"success": 1}


@user_org_api.post("/users/reset_password/")
def post_reset_password(
    request, payload: ResetPasswordSchema
):  # pylint: disable=unused-argument
    """step 2 of the forgot-password flow"""
    redis = Redis()
    redis_key = f"password-reset:{payload.token}"
    password_reset = redis.get(redis_key)
    if password_reset is None:
        raise HttpError(400, "invalid reset code")

    redis.delete(redis_key)
    orguserid_str = password_reset.decode("utf8")
    orguser = OrgUser.objects.filter(id=int(orguserid_str)).first()
    if orguser is None:
        logger.error("no orguser having id %s", orguserid_str)
        raise HttpError(400, "could not look up request from this token")

    orguser.user.set_password(payload.password.get_secret_value())
    orguser.user.save()

    return {"success": 1}


@user_org_api.get("/users/verify_email/resend", auth=auth.AnyOrgUser())
def get_verify_email_resend(request):  # pylint: disable=unused-argument
    """this api is hit when the user is logged in but the email is still not verified"""
    redis = Redis()
    token = uuid4()

    redis_key = f"email-verification:{token.hex}"
    orguserid_bytes = str(request.orguser.id).encode("utf8")

    redis.set(redis_key, orguserid_bytes)

    FRONTEND_URL = os.getenv("FRONTEND_URL")
    reset_url = f"{FRONTEND_URL}/verifyemail/?token={token.hex}"
    try:
        sendgrid.send_signup_email(request.user.email, reset_url)
    except Exception as error:
        raise HttpError(400, "failed to send email") from error

    return {"success": 1}


@user_org_api.post("/users/verify_email/")
def post_verify_email(
    request, payload: VerifyEmailSchema
):  # pylint: disable=unused-argument
    """step 2 of the verify-email flow"""
    redis = Redis()
    redis_key = f"email-verification:{payload.token}"
    verify_email = redis.get(redis_key)
    if verify_email is None:
        raise HttpError(400, "this link has expired")

    redis.delete(redis_key)
    orguserid_str = verify_email.decode("utf8")
    orguser = OrgUser.objects.filter(id=int(orguserid_str)).first()
    if orguser is None:
        logger.error("no orguser having id %s", orguserid_str)
        raise HttpError(400, "could not look up request from this token")

    # verify email for all the orgusers
    OrgUser.objects.filter(user_id=orguser.user.id).update(email_verified=True)

    return {"success": 1}
