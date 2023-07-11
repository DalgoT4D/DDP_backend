from datetime import datetime
from typing import List
from uuid import uuid4
import json
import os
from dotenv import load_dotenv
from redis import Redis

from django.contrib.auth.models import User
from django.utils.text import slugify
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
    OrgUserResponse,
    OrgUserRole,
    OrgUserUpdate,
    ForgotPasswordSchema,
    ResetPasswordSchema,
    VerifyEmailSchema,
)
from ddpui.ddpprefect import prefect_service
from ddpui.ddpairbyte import airbyte_service, airbytehelpers
from ddpui.ddpdbt import dbt_service
from ddpui.ddpprefect import AIRBYTECONNECTION
from ddpui.utils.ddp_logger import logger
from ddpui.utils.timezone import IST
from ddpui.utils import secretsmanager
from ddpui.utils import sendgrid
from ddpui.utils import helpers

user_org_api = NinjaAPI(urls_namespace="userorg")
# http://127.0.0.1:8000/api/docs

load_dotenv()


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
    return Response({"detail": "something went wrong"}, status=500)


@user_org_api.get("/currentuser", response=OrgUserResponse, auth=auth.AnyOrgUser())
def get_current_user(request):
    """return the OrgUser making this request"""
    orguser = request.orguser
    if orguser is not None:
        return OrgUserResponse.from_orguser(orguser)
    raise HttpError(400, "requestor is not an OrgUser")


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
        f"with slug: {org.slug}"
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
        org = None
        orguser = OrgUser.objects.filter(user__email=request_obj["username"]).first()
        if orguser.org is not None:
            org = orguser.org.name
        return {
            "token": token.data["token"],
            "org": org,
            "email": str(orguser),
            "role": OrgUserRole(orguser.role).name,
            "active": orguser.user.is_active,
        }

    return token


@user_org_api.get(
    "/organizations/users", response=List[OrgUserResponse], auth=auth.CanManageUsers()
)
def get_organization_users(request):
    """list all OrgUsers in the requestor's org, including inactive"""
    orguser = request.orguser
    if orguser.org is None:
        raise HttpError(400, "no associated org")
    query = OrgUser.objects.filter(org=orguser.org)
    return [OrgUserResponse.from_orguser(orguser) for orguser in query]


@user_org_api.put(
    "/organizations/user_self/", response=OrgUserResponse, auth=auth.AnyOrgUser()
)
def put_organization_user_self(request, payload: OrgUserUpdate):
    """update the requestor's OrgUser"""
    orguser = request.orguser

    if payload.email:
        orguser.user.email = payload.email
    if payload.active is not None:
        orguser.user.is_active = payload.active
    orguser.user.save()

    logger.info(f"updated self {orguser.user.email} with slug: {org.slug}")
    return OrgUserResponse(
        email=orguser.user.email, active=orguser.user.is_active, role=orguser.role
    )


@user_org_api.put(
    "/organizations/users/",
    response=OrgUserResponse,
    auth=auth.CanManageUsers(),
)
def put_organization_user(request, payload: OrgUserUpdate):
    """update another OrgUser"""
    requestor_orguser = request.orguser

    if requestor_orguser.role not in [
        OrgUserRole.ACCOUNT_MANAGER,
        OrgUserRole.PIPELINE_MANAGER,
    ]:
        raise HttpError(400, "not authorized to update another user")

    orguser = OrgUser.objects.filter(
        user__email=payload.toupdate_email, org=request.orguser.org
    ).first()

    if payload.email:
        orguser.user.email = payload.email
    if payload.active is not None:
        orguser.user.is_active = payload.active
    if payload.role:
        orguser.role = payload.role
    orguser.user.save()

    logger.info(f"updated orguser {orguser.user.email} with slug: {org.slug}")
    return OrgUserResponse(
        email=orguser.user.email, active=orguser.user.is_active, role=orguser.role
    )


@user_org_api.post("/organizations/", response=OrgSchema, auth=auth.FullAccess())
def post_organization(request, payload: OrgSchema):
    """creates a new org and attaches it to the requestor"""
    orguser = request.orguser
    if orguser.org:
        raise HttpError(400, "orguser already has an associated org")
    org = Org.objects.filter(name=payload.name).first()
    if org:
        raise HttpError(400, "client org with this name already exists")
    org = Org.objects.create(**payload.dict())
    org.slug = slugify(org.name)[:20]
    org.save()
    logger.info(f"{orguser.user.email} created new org {org.name} with slug: {org.slug}")
    try:
        new_workspace = airbytehelpers.setup_airbyte_workspace(org.slug, org)
    except Exception as error:
        # delete the org or we won't be able to create it once airbyte comes back up
        org.delete()
        raise HttpError(400, "could not create airbyte workspace") from error
    orguser.org = org
    orguser.save()
    return OrgSchema(name=org.name, airbyte_workspace_id=new_workspace.workspaceId)


@user_org_api.post("/organizations/warehouse/", auth=auth.CanManagePipelines())
def post_organization_warehouse(request, payload: OrgWarehouseSchema):
    """registers a data warehouse for the org"""
    orguser = request.orguser
    if payload.wtype not in ["postgres", "bigquery"]:
        raise HttpError(400, "unrecognized warehouse type " + payload.wtype)

    destination = airbyte_service.create_destination(
        orguser.org.airbyte_workspace_id,
        f"{payload.wtype}-warehouse",
        payload.destinationDefId,
        payload.airbyteConfig,
    )
    logger.info("created destination having id " + destination["destinationId"]+" with slug: "+{org.slug})

    # prepare the dbt credentials from airbyteConfig
    dbtCredenials = None
    if payload.wtype == "postgres":
        dbtCredenials = {
            "host": payload.airbyteConfig["host"],
            "port": payload.airbyteConfig["port"],
            "username": payload.airbyteConfig["username"],
            "password": payload.airbyteConfig["password"],
            "database": payload.airbyteConfig["database"],
        }

    if payload.wtype == "bigquery":
        dbtCredenials = json.loads(payload.airbyteConfig["credentials_json"])

    warehouse = OrgWarehouse(
        org=orguser.org,
        wtype=payload.wtype,
        credentials="",
        airbyte_destination_id=destination["destinationId"],
    )
    credentials_lookupkey = secretsmanager.save_warehouse_credentials(
        warehouse, dbtCredenials
    )
    warehouse.credentials = credentials_lookupkey
    warehouse.save()
    return {"success": 1}


@user_org_api.delete("/organizations/warehouses/", auth=auth.CanManagePipelines())
def delete_organization_warehouses(request):
    """deletes all (references to) data warehouses for the org"""
    orguser = request.orguser
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
    orguser = request.orguser
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
    orguser = request.orguser
    invitation = Invitation.objects.filter(invited_email=payload.invited_email).first()
    if invitation:
        logger.error(
            f"{payload.invited_email} has already been invited by "
            f"{invitation.invited_by} on {invitation.invited_on.strftime('%Y-%m-%d')}"
        )
        raise HttpError(400, f"{payload.invited_email} has already been invited")

    payload.invited_by = OrgUserResponse.from_orguser(orguser)
    payload.invited_on = datetime.now(IST)
    payload.invite_code = str(uuid4())
    invitation = Invitation.objects.create(
        invited_email=payload.invited_email,
        invited_role=payload.invited_role,
        invited_by=orguser,
        invited_on=payload.invited_on,
        invite_code=payload.invite_code,
    )
    logger.info("created Invitation with slug: "+ org.slug)
    return payload


# the invitee will get a hyperlink via email, clicking will take them to \
# the UI where they will choose
# a password, then click a button POSTing to this endpoint
@user_org_api.get(
    "/organizations/users/invite/{invite_code}",
    response=InvitationSchema,
)
def get_organization_user_invite(
    request, invite_code
):  # pylint: disable=unused-argument
    """Fetch the invite sent to user with a particular invite code"""
    invitation = Invitation.objects.filter(invite_code=invite_code).first()
    if invitation is None:
        raise HttpError(400, "invalid invite code")
    return InvitationSchema.from_invitation(invitation)


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
    orguser = OrgUser.objects.filter(
        user__email=invitation.invited_email, org=invitation.invited_by.org
    ).first()
    if not orguser:
        logger.info(
            f"creating invited user {invitation.invited_email}"
            f"for {invitation.invited_by.org.name}"
            f"with slug: {org.slug}"
        )
        user = User.objects.create_user(
            username=invitation.invited_email,
            email=invitation.invited_email,
            password=payload.password,
        )
        orguser = OrgUser.objects.create(
            user=user, org=invitation.invited_by.org, role=invitation.invited_role
        )
    return OrgUserResponse.from_orguser(orguser)


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

    orguser.email_verified = True
    orguser.save()

    return {"success": 1}
